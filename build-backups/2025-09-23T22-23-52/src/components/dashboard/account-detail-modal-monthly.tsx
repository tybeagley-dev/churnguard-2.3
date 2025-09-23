import { useState } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceDot, ReferenceArea, ReferenceLine } from "recharts";
import { Flag } from "lucide-react";
import { useMonthlyAccountHistory } from "@/hooks/use-monthly-accounts";

interface AccountDetailModalProps {
  accountId: string | null;
  accountName: string;
  isOpen: boolean;
  onClose: () => void;
}

interface MonthlyMetric {
  month_yr: string;
  month_label: string;
  total_spend: number;
  total_texts_delivered: number;
  coupons_redeemed: number;
  active_subs_cnt: number;
  risk_level?: string;
  risk_reasons?: string[];
  risk_flags?: {
    monthlyRedemptionsFlag: boolean;
    lowActivityFlag: boolean;
    spendDropFlag: boolean;
    redemptionsDropFlag: boolean;
  };
}

export default function AccountDetailModal({ 
  accountId, 
  accountName, 
  isOpen, 
  onClose 
}: AccountDetailModalProps) {
  const [visibleMetrics, setVisibleMetrics] = useState({
    spend: true,
    texts: true,
    coupons: true,
    subscriptions: true
  });

  const { data: monthlyData, isLoading } = useMonthlyAccountHistory(accountId || '');

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', { 
      style: 'currency', 
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(value);
  };

  const formatNumber = (value: number) => {
    return new Intl.NumberFormat('en-US').format(value);
  };

  const toggleMetric = (metric: keyof typeof visibleMetrics) => {
    setVisibleMetrics(prev => ({
      ...prev,
      [metric]: !prev[metric]
    }));
  };

  // Calculate risk levels for each month based on thresholds
  const calculateRiskForMonth = (currentMonth: MonthlyMetric, previousMonth?: MonthlyMetric, monthsSinceStart?: number) => {
    const MONTHLY_REDEMPTIONS_THRESHOLD = 3;
    const LOW_ACTIVITY_SUBSCRIBERS_THRESHOLD = 300;
    const LOW_ACTIVITY_REDEMPTIONS_THRESHOLD = 35;
    const SPEND_DROP_THRESHOLD = 0.40; // 40%
    const REDEMPTIONS_DROP_THRESHOLD = 0.50; // 50%

    let flagCount = 0;
    const flags = {
      monthlyRedemptionsFlag: false,
      lowActivityFlag: false,
      spendDropFlag: false,
      redemptionsDropFlag: false,
    };

    // Flag 1: Monthly Redemptions (≤ 3 redemptions)
    if (currentMonth.coupons_redeemed <= MONTHLY_REDEMPTIONS_THRESHOLD) {
      flags.monthlyRedemptionsFlag = true;
      flagCount++;
    }

    // Flag 2: Low Activity (< 300 subscribers AND < 35 redemptions)
    if (currentMonth.active_subs_cnt < LOW_ACTIVITY_SUBSCRIBERS_THRESHOLD && 
        currentMonth.coupons_redeemed < LOW_ACTIVITY_REDEMPTIONS_THRESHOLD) {
      flags.lowActivityFlag = true;
      flagCount++;
    }

    // Flag 3 & 4: Only calculate drops if we have previous month data AND this is at least month 3
    // (Drop flags should only apply after month 2, since you need a baseline to compare against)
    if (previousMonth && monthsSinceStart !== undefined && monthsSinceStart >= 3) {
      // Flag 3: Spend Drop (≥ 40% decrease)
      const spendDrop = previousMonth.total_spend > 0 
        ? Math.max(0, (previousMonth.total_spend - currentMonth.total_spend) / previousMonth.total_spend)
        : 0;
      if (spendDrop >= SPEND_DROP_THRESHOLD) {
        flags.spendDropFlag = true;
        flagCount++;
      }

      // Flag 4: Redemptions Drop (≥ 50% decrease)
      const redemptionsDrop = previousMonth.coupons_redeemed > 0 
        ? Math.max(0, (previousMonth.coupons_redeemed - currentMonth.coupons_redeemed) / previousMonth.coupons_redeemed)
        : 0;
      if (redemptionsDrop >= REDEMPTIONS_DROP_THRESHOLD) {
        flags.redemptionsDropFlag = true;
        flagCount++;
      }
    }

    // Determine risk level
    let riskLevel = 'low';
    if (flagCount >= 3) riskLevel = 'high';
    else if (flagCount >= 1) riskLevel = 'medium';

    return { riskLevel, flags, flagCount };
  };

  // Determine current month (format: YYYY-MM)
  const currentMonth = new Date().toISOString().slice(0, 7); // "2025-09"

  // Check if we have current month data for dotted lines
  const hasCurrentMonthData = monthlyData?.some(item => item.month_yr === currentMonth) || false;

  const chartData = monthlyData?.map((item: MonthlyMetric, index: number) => {
    const previousMonth = index > 0 ? monthlyData[index - 1] : undefined;

    // Calculate months of operation based on actual data
    // Find the first month with any activity (non-zero spend, texts, or redemptions)
    const firstActiveMonthIndex = monthlyData.findIndex(month =>
      month.total_spend > 0 || month.total_texts_delivered > 0 || month.coupons_redeemed > 0
    );

    // Calculate months since first activity (0-based, so +1 for actual month count)
    const monthsSinceFirstActivity = firstActiveMonthIndex >= 0 ? index - firstActiveMonthIndex + 1 : 0;

    const riskData = calculateRiskForMonth(item, previousMonth, monthsSinceFirstActivity);
    const isCurrentMonth = item.month_yr === currentMonth;

    return {
      month: item.month_label,
      spend: item.total_spend,
      texts: item.total_texts_delivered,
      coupons: item.coupons_redeemed,
      subscriptions: item.active_subs_cnt,
      riskLevel: item.risk_level || 'low', // Use database historical_risk_level
      riskReasons: (() => {
        if (!item.risk_reasons) return ['No flags'];
        if (Array.isArray(item.risk_reasons)) return item.risk_reasons;
        if (typeof item.risk_reasons === 'string') {
          try {
            const parsed = JSON.parse(item.risk_reasons);
            return Array.isArray(parsed) ? parsed : [item.risk_reasons];
          } catch {
            return item.risk_reasons.includes(',')
              ? item.risk_reasons.split(',').map(r => r.trim())
              : [item.risk_reasons];
          }
        }
        return ['No flags'];
      })(),
      hasRiskFlag: (item.risk_level && item.risk_level !== 'low') || riskData.flagCount > 0,
      flagCount: riskData.flagCount,
      riskFlags: riskData.flags,
      monthIndex: index, // Add index for ReferenceArea positioning
      isCurrentMonth: isCurrentMonth // Flag for visual distinction
    };
  }).reverse() || []; // Reverse to show oldest → newest (left to right)


  // Helper function to get risk color (matching the legend squares)
  const getRiskColor = (riskLevel: string) => {
    switch (riskLevel?.toLowerCase()) {
      case 'high':
        return 'rgba(220, 38, 38, 0.15)'; // red-600 with 15% opacity
      case 'medium':
        return 'rgba(234, 88, 12, 0.15)'; // orange-600 with 15% opacity
      case 'low':
      default:
        return 'rgba(34, 197, 94, 0.15)'; // green-600 with 15% opacity
    }
  };

  // Calculate summary metrics from current month (first item since data comes DESC from DB)
  const currentMonthData = monthlyData?.[0]; // First item is newest month from ORDER BY month DESC
  const summaryMetrics = currentMonthData ? {
    spend: currentMonthData.total_spend,
    texts: currentMonthData.total_texts_delivered,
    coupons: currentMonthData.coupons_redeemed,
    subscriptions: currentMonthData.active_subs_cnt
  } : { spend: 0, texts: 0, coupons: 0, subscriptions: 0 };

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="text-xl font-semibold">
            {accountName} - Monthly Performance
          </DialogTitle>
        </DialogHeader>

        {isLoading ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-lg">Loading monthly data...</div>
          </div>
        ) : (
          <div className="space-y-6">
            {/* Summary Cards */}
            <div className="grid grid-cols-4 gap-4">
              <Card>
                <CardContent className="p-4">
                  <div className="text-sm font-medium text-gray-600">Monthly Spend</div>
                  <div className="text-2xl font-bold text-purple-600">
                    {formatCurrency(summaryMetrics.spend)}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="text-sm font-medium text-gray-600">Texts Sent</div>
                  <div className="text-2xl font-bold text-orange-600">
                    {formatNumber(summaryMetrics.texts)}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="text-sm font-medium text-gray-600">Redemptions</div>
                  <div className="text-2xl font-bold text-green-600">
                    {formatNumber(summaryMetrics.coupons)}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="p-4">
                  <div className="text-sm font-medium text-gray-600">Subscribers</div>
                  <div className="text-2xl font-bold text-blue-600">
                    {formatNumber(summaryMetrics.subscriptions)}
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* 12-Month Performance Trends Chart */}
            <Card>
              <CardHeader>
                <CardTitle>12-Month Performance Trends</CardTitle>
                <div className="flex items-center space-x-6">
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="spend-toggle"
                      checked={visibleMetrics.spend}
                      onCheckedChange={() => toggleMetric('spend')}
                    />
                    <label htmlFor="spend-toggle" className="text-sm font-medium text-purple-600">
                      Monthly Spend
                    </label>
                  </div>

                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="texts-toggle"
                      checked={visibleMetrics.texts}
                      onCheckedChange={() => toggleMetric('texts')}
                    />
                    <label htmlFor="texts-toggle" className="text-sm font-medium text-orange-600">
                      Texts Sent
                    </label>
                  </div>

                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="coupons-toggle"
                      checked={visibleMetrics.coupons}
                      onCheckedChange={() => toggleMetric('coupons')}
                    />
                    <label htmlFor="coupons-toggle" className="text-sm font-medium text-green-600">
                      Redemptions
                    </label>
                  </div>

                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="subscriptions-toggle"
                      checked={visibleMetrics.subscriptions}
                      onCheckedChange={() => toggleMetric('subscriptions')}
                    />
                    <label htmlFor="subscriptions-toggle" className="text-sm font-medium text-blue-600">
                      Subscribers
                    </label>
                  </div>
                </div>
                
                {/* Risk Flag Legend */}
                <div className="flex items-center space-x-4 text-xs text-gray-600 mt-2">
                  <span>Risk Indicators:</span>
                  <div className="flex items-center space-x-1">
                    <div className="w-3 h-3 bg-red-600"></div>
                    <span>High Risk</span>
                  </div>
                  <div className="flex items-center space-x-1">
                    <div className="w-3 h-3 bg-orange-600"></div>
                    <span>Medium Risk</span>
                  </div>
                  <div className="flex items-center space-x-1">
                    <div className="w-3 h-3 bg-green-600"></div>
                    <span>Low Risk</span>
                  </div>
                </div>
              </CardHeader>

              <CardContent>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis 
                        dataKey="month" 
                        tick={{ fontSize: 12 }}
                        angle={-45}
                        textAnchor="end"
                        height={80}
                      />
                      <YAxis tick={{ fontSize: 12 }} />
                      <Tooltip
                        content={({ active, payload, label, coordinate }) => {
                          if (!active || !payload || !payload.length) return null;

                          // Find the closest data point based on the tooltip's position
                          let closestDataPoint = chartData.find(d => d.month === label);

                          // If we can't find an exact match, find the nearest data point
                          if (!closestDataPoint && coordinate) {
                            const chartWidth = coordinate.x || 0;
                            const dataPointWidth = chartWidth / chartData.length;
                            const hoveredIndex = Math.round(chartWidth / dataPointWidth);
                            const clampedIndex = Math.max(0, Math.min(hoveredIndex, chartData.length - 1));
                            closestDataPoint = chartData[clampedIndex];
                          }

                          // Fallback to first data point if still no match
                          if (!closestDataPoint) {
                            closestDataPoint = chartData[0];
                          }

                          return (
                            <div className="bg-white p-3 border border-gray-200 rounded-lg shadow-lg">
                              <div className="font-medium text-gray-900 mb-2">
                                Month: {label || closestDataPoint?.month}
                              </div>

                              {/* Metric Values */}
                              <div className="space-y-1">
                                {payload.map((entry: any) => {
                                  const formatValue = entry.dataKey === 'spend' ? formatCurrency(entry.value) : formatNumber(entry.value);
                                  const metricLabel = {
                                    spend: 'Monthly Spend',
                                    texts: 'Texts Sent',
                                    coupons: 'Redemptions',
                                    subscriptions: 'Subscribers'
                                  }[entry.dataKey] || entry.dataKey;

                                  return (
                                    <div key={entry.dataKey} className="flex items-center">
                                      <div
                                        className="w-3 h-3 rounded-full mr-2"
                                        style={{ backgroundColor: entry.color }}
                                      />
                                      <span className="text-sm">
                                        {metricLabel}: <span className="font-medium">{formatValue}</span>
                                      </span>
                                    </div>
                                  );
                                })}
                              </div>

                              {/* Risk Information */}
                              {closestDataPoint && (
                                <div className="mt-3 pt-2 border-t border-gray-100">
                                  <div className="flex items-center mb-1">
                                    <div
                                      className={`w-3 h-3 mr-2 ${
                                        closestDataPoint.riskLevel === 'high' ? 'bg-red-600' :
                                        closestDataPoint.riskLevel === 'medium' ? 'bg-orange-600' : 'bg-green-600'
                                      }`}
                                    />
                                    <span className="text-sm font-medium text-gray-900">
                                      {closestDataPoint.riskLevel?.toUpperCase() || 'LOW'} RISK
                                    </span>
                                  </div>
                                  <div className="text-xs text-gray-600 ml-5 space-y-1">
                                    {closestDataPoint.riskReasons && Array.isArray(closestDataPoint.riskReasons) ? (
                                      closestDataPoint.riskReasons.map((reason: string, idx: number) => (
                                        <div key={idx}>• {reason}</div>
                                      ))
                                    ) : (
                                      <div>• No flags</div>
                                    )}
                                  </div>
                                </div>
                              )}
                            </div>
                          );
                        }}
                      />

                      {/* Background colored areas for risk levels */}
                      {chartData.map((dataPoint, index) => {
                        const currentMonth = dataPoint.month;
                        const nextMonth = index < chartData.length - 1 ? chartData[index + 1].month : null;

                        const x1 = currentMonth;
                        const x2 = nextMonth || currentMonth;

                        const fillColor = getRiskColor(dataPoint.riskLevel);

                        return (
                          <ReferenceArea
                            key={`risk-bg-${index}`}
                            x1={x1}
                            x2={x2}
                            fill={fillColor}
                            fillOpacity={1}
                          />
                        );
                      })}

                      {/* Double line boundary between completed months and current month */}
                      {(() => {
                        const currentMonthIndex = chartData.findIndex(d => d.isCurrentMonth);
                        if (currentMonthIndex > 0) {
                          const lastCompletedMonth = chartData[currentMonthIndex - 1].month;
                          return (
                            <>
                              <ReferenceLine
                                x={lastCompletedMonth}
                                stroke="#374151"
                                strokeWidth={3}
                                strokeDasharray="none"
                              />
                              <ReferenceLine
                                x={lastCompletedMonth}
                                stroke="#6b7280"
                                strokeWidth={1}
                                strokeDasharray="5 5"
                              />
                            </>
                          );
                        }
                        return null;
                      })()}

                      {visibleMetrics.spend && (
                        <Line
                          type="monotone"
                          dataKey="spend"
                          stroke="#8b5cf6"
                          strokeWidth={2}
                          dot={{ fill: '#8b5cf6', strokeWidth: 2, r: 4 }}
                          activeDot={{ fill: '#8b5cf6', stroke: '#8b5cf6', strokeWidth: 3, r: 6 }}
                        />
                      )}

                      {visibleMetrics.texts && (
                        <Line
                          type="monotone"
                          dataKey="texts"
                          stroke="#ea580c"
                          strokeWidth={2}
                          dot={{ fill: '#ea580c', strokeWidth: 2, r: 4 }}
                          activeDot={{ fill: '#ea580c', stroke: '#ea580c', strokeWidth: 3, r: 6 }}
                        />
                      )}

                      {visibleMetrics.coupons && (
                        <Line
                          type="monotone"
                          dataKey="coupons"
                          stroke="#059669"
                          strokeWidth={2}
                          dot={{ fill: '#059669', strokeWidth: 2, r: 4 }}
                          activeDot={{ fill: '#059669', stroke: '#059669', strokeWidth: 3, r: 6 }}
                        />
                      )}

                      {visibleMetrics.subscriptions && (
                        <Line
                          type="monotone"
                          dataKey="subscriptions"
                          stroke="#2563eb"
                          strokeWidth={2}
                          dot={{ fill: '#2563eb', strokeWidth: 2, r: 4 }}
                          activeDot={{ fill: '#2563eb', stroke: '#2563eb', strokeWidth: 3, r: 6 }}
                        />
                      )}
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>

            {/* Monthly Breakdown Table */}
            <Card>
              <CardHeader>
                <CardTitle>Monthly Breakdown</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Month</th>
                        <th className="text-right py-3 px-4 font-medium text-gray-600">Spend</th>
                        <th className="text-right py-3 px-4 font-medium text-gray-600">Texts</th>
                        <th className="text-right py-3 px-4 font-medium text-gray-600">Redemptions</th>
                        <th className="text-right py-3 px-4 font-medium text-gray-600">Subscribers</th>
                      </tr>
                    </thead>
                    <tbody>
                      {monthlyData?.map((month: MonthlyMetric, index: number) => (
                        <tr key={month.month_yr} className="border-b hover:bg-gray-50">
                          <td className="py-3 px-4 font-medium">
                            {month.month_yr === currentMonth ? `${month.month_label} (MTD)` : month.month_label}
                          </td>
                          <td className="text-right py-3 px-4">{formatCurrency(month.total_spend)}</td>
                          <td className="text-right py-3 px-4">{formatNumber(month.total_texts_delivered)}</td>
                          <td className="text-right py-3 px-4">{formatNumber(month.coupons_redeemed)}</td>
                          <td className="text-right py-3 px-4">{formatNumber(month.active_subs_cnt)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}