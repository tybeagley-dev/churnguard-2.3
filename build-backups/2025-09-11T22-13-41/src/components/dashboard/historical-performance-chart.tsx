import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { Checkbox } from '@/components/ui/checkbox';


interface HistoricalPerformanceData {
  month: string;
  monthLabel: string;
  totalSpend: number;
  totalRedemptions: number;
  totalSubscribers: number;
  totalTexts: number;
  accountCount: number;
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

export function HistoricalPerformanceChart() {
  const { data: historicalData, isLoading, error } = useQuery<HistoricalPerformanceData[]>({
    queryKey: ['/api/bigquery/historical-performance'],
    staleTime: 5 * 60 * 1000, // 5 minutes
    gcTime: 10 * 60 * 1000, // 10 minutes
  });

  // State for toggling metrics visibility
  const [visibleMetrics, setVisibleMetrics] = useState({
    totalSpend: true,
    totalRedemptions: true,
    totalSubscribers: false,
    totalTexts: false,
  });

  const toggleMetric = (metric: keyof typeof visibleMetrics) => {
    setVisibleMetrics(prev => ({
      ...prev,
      [metric]: !prev[metric]
    }));
  };

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Historical Performance</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-96 flex items-center justify-center">
            <div className="text-gray-500">Loading historical performance data...</div>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Historical Performance</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-96 flex items-center justify-center">
            <div className="text-red-500">Error loading historical performance data</div>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (!historicalData || historicalData.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Historical Performance</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-96 flex items-center justify-center">
            <div className="text-gray-500">No historical performance data available</div>
          </div>
        </CardContent>
      </Card>
    );
  }

  // Prepare chart data with month labels for display
  const chartData = historicalData.map(item => ({
    ...item,
    displayMonth: item.monthLabel.split(' ')[0] // Just show month name
  }));

  // Prepare chart data with month labels for display

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white border rounded-lg shadow-lg p-3">
          <p className="font-semibold">{data.monthLabel}</p>
          {visibleMetrics.totalSpend && (
            <p className="text-purple-600">
              Total Spend: {formatCurrency(data.totalSpend)}
            </p>
          )}
          {visibleMetrics.totalRedemptions && (
            <p className="text-orange-600">
              Total Redemptions: {formatNumber(data.totalRedemptions)}
            </p>
          )}
          {visibleMetrics.totalSubscribers && (
            <p className="text-green-600">
              Total Subscribers: {formatNumber(data.totalSubscribers)}
            </p>
          )}
          {visibleMetrics.totalTexts && (
            <p className="text-blue-600">
              Total Texts: {formatNumber(data.totalTexts)}
            </p>
          )}
          <p className="text-gray-600">
            Accounts: {formatNumber(data.accountCount)}
          </p>
        </div>
      );
    }
    return null;
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between">
          <span>Historical Performance</span>
          <div className="text-sm text-gray-600">
            Past 9 Months
          </div>
        </CardTitle>
      </CardHeader>
      
      <CardContent>
        {/* Metric selection checkboxes */}
        <div className="flex flex-wrap gap-4 mb-4 p-3 bg-gray-50 rounded-lg">
          <div className="flex items-center space-x-2">
            <Checkbox
              id="totalSpend"
              checked={visibleMetrics.totalSpend}
              onCheckedChange={() => toggleMetric('totalSpend')}
            />
            <label htmlFor="totalSpend" className="text-sm font-medium cursor-pointer text-purple-600">
              Total Spend
            </label>
          </div>
          <div className="flex items-center space-x-2">
            <Checkbox
              id="totalRedemptions"
              checked={visibleMetrics.totalRedemptions}
              onCheckedChange={() => toggleMetric('totalRedemptions')}
            />
            <label htmlFor="totalRedemptions" className="text-sm font-medium cursor-pointer text-orange-600">
              Total Redemptions
            </label>
          </div>
          <div className="flex items-center space-x-2">
            <Checkbox
              id="totalSubscribers"
              checked={visibleMetrics.totalSubscribers}
              onCheckedChange={() => toggleMetric('totalSubscribers')}
            />
            <label htmlFor="totalSubscribers" className="text-sm font-medium cursor-pointer text-green-600">
              Total Subscribers
            </label>
          </div>
          <div className="flex items-center space-x-2">
            <Checkbox
              id="totalTexts"
              checked={visibleMetrics.totalTexts}
              onCheckedChange={() => toggleMetric('totalTexts')}
            />
            <label htmlFor="totalTexts" className="text-sm font-medium cursor-pointer text-blue-600">
              Total Texts
            </label>
          </div>
        </div>

        <div className="h-96">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis 
                dataKey="displayMonth" 
                stroke="#666"
                fontSize={12}
              />
              <YAxis 
                yAxisId="left"
                orientation="left"
                stroke="#666"
                fontSize={12}
                tickFormatter={(value) => {
                  if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
                  if (value >= 1000) return `${(value / 1000).toFixed(0)}k`;
                  return value.toString();
                }}
              />
              <Tooltip content={<CustomTooltip />} />
              {visibleMetrics.totalSpend && (
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="totalSpend"
                  stroke="#8b5cf6"
                  strokeWidth={3}
                  dot={{ fill: "#8b5cf6", strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6, fill: "#8b5cf6" }}
                  name="Total Spend"
                />
              )}
              {visibleMetrics.totalRedemptions && (
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="totalRedemptions"
                  stroke="#f97316"
                  strokeWidth={3}
                  dot={{ fill: "#f97316", strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6, fill: "#f97316" }}
                  name="Total Redemptions"
                />
              )}
              {visibleMetrics.totalSubscribers && (
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="totalSubscribers"
                  stroke="#16a34a"
                  strokeWidth={3}
                  dot={{ fill: "#16a34a", strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6, fill: "#16a34a" }}
                  name="Total Subscribers"
                />
              )}
              {visibleMetrics.totalTexts && (
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="totalTexts"
                  stroke="#2563eb"
                  strokeWidth={3}
                  dot={{ fill: "#2563eb", strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6, fill: "#2563eb" }}
                  name="Total Texts"
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}