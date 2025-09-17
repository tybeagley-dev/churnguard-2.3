import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { useState } from 'react';
import { Checkbox } from '@/components/ui/checkbox';

export interface Claude12MonthData {
  month_label: string;
  month_yr: string;
  total_accounts: number;
  accounts_below_minimum: number;
  spend_original: number;
  spend_adjusted: number;
  spend_adjustment: number;
  total_redemptions: number;
  total_subscribers: number;
  total_texts_sent: number;
  spend_change_pct: number | null;
  accounts_change_pct: number | null;
  total_spend_12mo: number;
  total_adjustment_12mo: number;
  total_redemptions_12mo: number;
  total_texts_12mo: number;
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat('en-US').format(value);
}

function formatLargeNumber(value: number): string {
  if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(0)}k`;
  return value.toString();
}

export function Claude12MonthChart() {
  const [selectedMetrics, setSelectedMetrics] = useState({
    spend_adjusted: true,
    total_accounts: true,
    total_redemptions: true,
    total_subscribers: true,
    total_texts_sent: true,
  });

  const { data: claude12MonthData, isLoading, error } = useQuery<Claude12MonthData[]>({
    queryKey: ['/api/historical-performance'],
    queryFn: async () => {
      const response = await fetch(`/api/historical-performance`);
      if (!response.ok) {
        throw new Error('Failed to fetch claude 12-month data');
      }
      return response.json();
    },
    staleTime: 0, // Force fresh data
    gcTime: 0, // No cache
    retry: 1,
    retryDelay: 5000,
    refetchOnWindowFocus: false,
    refetchOnMount: false,
  });

  const toggleMetric = (metric: keyof typeof selectedMetrics) => {
    setSelectedMetrics(prev => ({
      ...prev,
      [metric]: !prev[metric],
    }));
  };

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Historical Performance (rolling 12 months)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-96 flex flex-col items-center justify-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mb-4"></div>
            <div className="text-gray-600 text-center">
              <div className="text-lg font-semibold mb-2">Processing 12-month BigQuery data...</div>
              <div className="text-sm text-gray-500">
                This may take 10-30 seconds as we analyze historical data across multiple tables
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Historical Performance (rolling 12 months)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-96 flex items-center justify-center">
            <div className="text-red-500">Error loading historical performance data</div>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (!claude12MonthData || claude12MonthData.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Historical Performance (rolling 12 months)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-96 flex items-center justify-center">
            <div className="text-gray-500">No historical performance data available</div>
          </div>
        </CardContent>
      </Card>
    );
  }

  // Get 12-month totals from the first record (all records have same totals)
  const firstRecord = claude12MonthData[0];
  const totalSpend12Mo = firstRecord.total_spend_12mo;
  const totalRedemptions12Mo = firstRecord.total_redemptions_12mo;
  const totalTexts12Mo = firstRecord.total_texts_12mo;
  const totalAccounts = Math.max(...claude12MonthData.map(d => d.total_accounts)); // Get peak account count

  const metrics = [
    { key: 'spend_adjusted', label: 'Spend Adjusted', color: '#8b5cf6', bgColor: 'bg-purple-50', textColor: 'text-purple-600' },
    { key: 'total_accounts', label: 'Total Accounts', color: '#f97316', bgColor: 'bg-orange-50', textColor: 'text-orange-600' },
    { key: 'total_redemptions', label: 'Total Redemptions', color: '#16a34a', bgColor: 'bg-green-50', textColor: 'text-green-600' },
    { key: 'total_subscribers', label: 'Total Subscribers', color: '#2563eb', bgColor: 'bg-blue-50', textColor: 'text-blue-600' },
    { key: 'total_texts_sent', label: 'Total Texts Sent', color: '#dc2626', bgColor: 'bg-red-50', textColor: 'text-red-600' },
  ];

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white border rounded-lg shadow-lg p-3">
          <p className="font-semibold">{data.month_label}</p>
          {selectedMetrics.spend_adjusted && (
            <p className="text-purple-600">
              Spend Adjusted: {formatCurrency(data.spend_adjusted)}
            </p>
          )}
          {selectedMetrics.total_accounts && (
            <p className="text-orange-600">
              Total Accounts: {formatNumber(data.total_accounts)}
            </p>
          )}
          {selectedMetrics.total_redemptions && (
            <p className="text-green-600">
              Total Redemptions: {formatNumber(data.total_redemptions)}
            </p>
          )}
          {selectedMetrics.total_subscribers && (
            <p className="text-blue-600">
              Total Subscribers: {formatNumber(data.total_subscribers)}
            </p>
          )}
          {selectedMetrics.total_texts_sent && (
            <p className="text-red-600">
              Total Texts Sent: {formatNumber(data.total_texts_sent)}
            </p>
          )}
        </div>
      );
    }
    return null;
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex justify-between items-center">
          <div>
            <CardTitle>Historical Performance</CardTitle>
            <p className="text-sm text-gray-600">Performance Tracking Across Primary Metrics | Rolling 12 months</p>
          </div>
        </div>
      </CardHeader>
      <CardContent>

        {/* Metric selection controls */}
        <div className="mb-6">
          <h4 className="text-sm font-semibold text-gray-700 mb-3">Select Metrics to Display:</h4>
          <div className="flex flex-wrap gap-4">
            {metrics.map((metric) => (
              <div key={metric.key} className="flex items-center space-x-2">
                <Checkbox
                  id={metric.key}
                  checked={selectedMetrics[metric.key as keyof typeof selectedMetrics]}
                  onCheckedChange={() => toggleMetric(metric.key as keyof typeof selectedMetrics)}
                />
                <label
                  htmlFor={metric.key}
                  className={`text-sm font-medium cursor-pointer ${metric.textColor}`}
                  style={{ color: metric.color }}
                >
                  {metric.label}
                </label>
              </div>
            ))}
          </div>
        </div>

        {/* Line chart with selectable metrics */}
        <div className="h-96">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={claude12MonthData} margin={{ top: 20, right: 30, left: 20, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis 
                dataKey="month_label" 
                stroke="#666"
                fontSize={12}
                angle={-45}
                textAnchor="end"
                height={60}
              />
              <YAxis 
                stroke="#666"
                fontSize={12}
                tickFormatter={(value) => formatLargeNumber(value)}
              />
              <Tooltip content={<CustomTooltip />} />
              
              {selectedMetrics.spend_adjusted && (
                <Line
                  type="monotone"
                  dataKey="spend_adjusted"
                  stroke="#8b5cf6"
                  strokeWidth={3}
                  dot={{ fill: "#8b5cf6", strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6, fill: "#8b5cf6" }}
                  name="Spend Adjusted"
                />
              )}
              
              {selectedMetrics.total_accounts && (
                <Line
                  type="monotone"
                  dataKey="total_accounts"
                  stroke="#f97316"
                  strokeWidth={3}
                  dot={{ fill: "#f97316", strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6, fill: "#f97316" }}
                  name="Total Accounts"
                />
              )}
              
              {selectedMetrics.total_redemptions && (
                <Line
                  type="monotone"
                  dataKey="total_redemptions"
                  stroke="#16a34a"
                  strokeWidth={3}
                  dot={{ fill: "#16a34a", strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6, fill: "#16a34a" }}
                  name="Total Redemptions"
                />
              )}
              
              {selectedMetrics.total_subscribers && (
                <Line
                  type="monotone"
                  dataKey="total_subscribers"
                  stroke="#2563eb"
                  strokeWidth={3}
                  dot={{ fill: "#2563eb", strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6, fill: "#2563eb" }}
                  name="Total Subscribers"
                />
              )}
              
              {selectedMetrics.total_texts_sent && (
                <Line
                  type="monotone"
                  dataKey="total_texts_sent"
                  stroke="#dc2626"
                  strokeWidth={3}
                  dot={{ fill: "#dc2626", strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6, fill: "#dc2626" }}
                  name="Total Texts Sent"
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}