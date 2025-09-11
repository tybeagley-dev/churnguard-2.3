import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, BarChart, Bar } from "recharts";

interface BigQueryTrendsChartProps {
  weeklyTrends: Array<{
    week: string;
    totalSpend: number;
    totalTexts: number;
    activeAccounts: number;
    churnedAccounts: number;
  }>;
}

export default function BigQueryTrendsChart({ weeklyTrends }: BigQueryTrendsChartProps) {
  if (!weeklyTrends || weeklyTrends.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Weekly Trends</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-12">
            <p className="text-gray-500">No trend data available</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  // Format data for charts
  const chartData = weeklyTrends.map(week => ({
    ...week,
    week: new Date(week.week).toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric' 
    }),
    totalSpendK: week.totalSpend / 1000, // Convert to thousands
    totalTextsK: week.totalTexts / 1000,
    churnRate: week.activeAccounts > 0 ? (week.churnedAccounts / week.activeAccounts) * 100 : 0
  }));

  const formatCurrency = (value: number) => 
    new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value * 1000);

  const formatNumber = (value: number) => 
    new Intl.NumberFormat('en-US').format(Math.round(value * 1000));

  const formatPercentage = (value: number) => `${value.toFixed(1)}%`;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* Revenue and Text Activity Trends */}
      <Card>
        <CardHeader>
          <CardTitle>Revenue & Text Activity Trends</CardTitle>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="week" />
              <YAxis yAxisId="left" orientation="left" />
              <YAxis yAxisId="right" orientation="right" />
              <Tooltip 
                formatter={(value, name) => {
                  if (name === 'totalSpendK') return [formatCurrency(value as number), 'Total Spend'];
                  if (name === 'totalTextsK') return [formatNumber(value as number), 'Total Texts'];
                  return [value, name];
                }}
              />
              <Legend />
              <Line 
                yAxisId="left"
                type="monotone" 
                dataKey="totalSpendK" 
                stroke="#10b981" 
                strokeWidth={2}
                name="Revenue ($K)"
              />
              <Line 
                yAxisId="right"
                type="monotone" 
                dataKey="totalTextsK" 
                stroke="#3b82f6" 
                strokeWidth={2}
                name="Text Volume (K)"
              />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Account Activity & Churn */}
      <Card>
        <CardHeader>
          <CardTitle>Account Activity & Churn</CardTitle>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="week" />
              <YAxis yAxisId="left" orientation="left" />
              <YAxis yAxisId="right" orientation="right" />
              <Tooltip 
                formatter={(value, name) => {
                  if (name === 'activeAccounts') return [value, 'Active Accounts'];
                  if (name === 'churnRate') return [formatPercentage(value as number), 'Churn Rate'];
                  return [value, name];
                }}
              />
              <Legend />
              <Bar 
                yAxisId="left"
                dataKey="activeAccounts" 
                fill="#8b5cf6" 
                name="Active Accounts"
              />
              <Line 
                yAxisId="right"
                type="monotone" 
                dataKey="churnRate" 
                stroke="#ef4444" 
                strokeWidth={2}
                name="Churn Rate (%)"
              />
            </BarChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Weekly Spend Distribution */}
      <Card>
        <CardHeader>
          <CardTitle>Weekly Spend Distribution</CardTitle>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="week" />
              <YAxis />
              <Tooltip 
                formatter={(value) => [formatCurrency(value as number), 'Weekly Spend']}
              />
              <Bar dataKey="totalSpendK" fill="#10b981" />
            </BarChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Text Activity by Week */}
      <Card>
        <CardHeader>
          <CardTitle>Text Activity by Week</CardTitle>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="week" />
              <YAxis />
              <Tooltip 
                formatter={(value) => [formatNumber(value as number), 'Text Messages']}
              />
              <Bar dataKey="totalTextsK" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>
    </div>
  );
}