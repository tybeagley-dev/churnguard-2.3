import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { TrendingUp, TrendingDown, Users, DollarSign, AlertTriangle, Activity } from "lucide-react";

interface BigQuerySummaryCardsProps {
  analytics: {
    totalAccounts: number;
    highRiskCount: number;
    mediumRiskCount: number;
    lowRiskCount: number;
    totalRevenue: number;
    revenueAtRisk: number;
    weeklyTrends: Array<{
      week: string;
      totalSpend: number;
      totalTexts: number;
      activeAccounts: number;
      churnedAccounts: number;
    }>;
  };
}

export default function BigQuerySummaryCards({ analytics }: BigQuerySummaryCardsProps) {
  if (!analytics) {
    return (
      <div className="text-center py-12">
        <p className="text-gray-500">Loading BigQuery data...</p>
      </div>
    );
  }

  const {
    totalAccounts,
    highRiskCount,
    mediumRiskCount,
    lowRiskCount,
    totalRevenue,
    revenueAtRisk,
    weeklyTrends
  } = analytics;

  // Calculate trends from weekly data
  const recentWeeks = weeklyTrends.slice(-4);
  const previousWeeks = weeklyTrends.slice(-8, -4);
  
  const avgRecentSpend = recentWeeks.reduce((sum, week) => sum + week.totalSpend, 0) / recentWeeks.length;
  const avgPreviousSpend = previousWeeks.reduce((sum, week) => sum + week.totalSpend, 0) / previousWeeks.length;
  const spendTrend = previousWeeks.length > 0 ? ((avgRecentSpend - avgPreviousSpend) / avgPreviousSpend) * 100 : 0;

  const avgRecentTexts = recentWeeks.reduce((sum, week) => sum + week.totalTexts, 0) / recentWeeks.length;
  const avgPreviousTexts = previousWeeks.reduce((sum, week) => sum + week.totalTexts, 0) / previousWeeks.length;
  const textTrend = previousWeeks.length > 0 ? ((avgRecentTexts - avgPreviousTexts) / avgPreviousTexts) * 100 : 0;

  const totalChurnedAccounts = recentWeeks.reduce((sum, week) => sum + week.churnedAccounts, 0);
  const churnRate = totalAccounts > 0 ? (totalChurnedAccounts / totalAccounts) * 100 : 0;

  const formatCurrency = (amount: number) => 
    new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(amount);

  const formatNumber = (num: number) => 
    new Intl.NumberFormat('en-US').format(Math.round(num));

  const formatPercentage = (percent: number) => 
    `${percent >= 0 ? '+' : ''}${percent.toFixed(1)}%`;

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
      {/* Total Accounts */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Total Accounts</CardTitle>
          <Users className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{formatNumber(totalAccounts)}</div>
          <p className="text-xs text-muted-foreground mt-1">
            Active restaurant clients
          </p>
        </CardContent>
      </Card>

      {/* High Risk Accounts */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">High Risk</CardTitle>
          <AlertTriangle className="h-4 w-4 text-red-500" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold text-red-600">{formatNumber(highRiskCount)}</div>
          <p className="text-xs text-muted-foreground mt-1">
            {totalAccounts > 0 ? ((highRiskCount / totalAccounts) * 100).toFixed(1) : 0}% of total accounts
          </p>
        </CardContent>
      </Card>

      {/* Total Revenue */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Total Revenue</CardTitle>
          <DollarSign className="h-4 w-4 text-green-500" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{formatCurrency(totalRevenue)}</div>
          <p className="text-xs text-muted-foreground mt-1 flex items-center">
            {spendTrend !== 0 && (
              <>
                {spendTrend > 0 ? (
                  <TrendingUp className="h-3 w-3 text-green-500 mr-1" />
                ) : (
                  <TrendingDown className="h-3 w-3 text-red-500 mr-1" />
                )}
                <span className={spendTrend > 0 ? 'text-green-600' : 'text-red-600'}>
                  {formatPercentage(spendTrend)}
                </span>
              </>
            )}
            {spendTrend !== 0 && <span className="ml-1">vs last month</span>}
          </p>
        </CardContent>
      </Card>

      {/* Revenue at Risk */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Revenue at Risk</CardTitle>
          <AlertTriangle className="h-4 w-4 text-orange-500" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold text-orange-600">{formatCurrency(revenueAtRisk)}</div>
          <p className="text-xs text-muted-foreground mt-1">
            {totalRevenue > 0 ? ((revenueAtRisk / totalRevenue) * 100).toFixed(1) : 0}% of total revenue
          </p>
        </CardContent>
      </Card>

      {/* Text Activity */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Text Activity</CardTitle>
          <Activity className="h-4 w-4 text-blue-500" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{formatNumber(avgRecentTexts)}</div>
          <p className="text-xs text-muted-foreground mt-1 flex items-center">
            {textTrend !== 0 && (
              <>
                {textTrend > 0 ? (
                  <TrendingUp className="h-3 w-3 text-green-500 mr-1" />
                ) : (
                  <TrendingDown className="h-3 w-3 text-red-500 mr-1" />
                )}
                <span className={textTrend > 0 ? 'text-green-600' : 'text-red-600'}>
                  {formatPercentage(textTrend)}
                </span>
              </>
            )}
            {textTrend !== 0 && <span className="ml-1">avg/week</span>}
          </p>
        </CardContent>
      </Card>

      {/* Churn Rate */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Churn Rate</CardTitle>
          <TrendingDown className="h-4 w-4 text-red-500" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold text-red-600">{churnRate.toFixed(1)}%</div>
          <p className="text-xs text-muted-foreground mt-1">
            {totalChurnedAccounts} accounts churned recently
          </p>
        </CardContent>
      </Card>
    </div>
  );
}