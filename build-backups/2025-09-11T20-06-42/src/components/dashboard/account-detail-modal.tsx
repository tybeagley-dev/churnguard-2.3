import { useState } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { useQuery } from '@tanstack/react-query';
import { Loader2 } from 'lucide-react';

interface AccountDetailModalProps {
  accountId: string | null;
  accountName: string;
  isOpen: boolean;
  onClose: () => void;
}

interface WeeklyMetric {
  week_yr: string;
  week_label: string;
  total_spend: number;
  total_texts_delivered: number;
  coupons_redeemed: number;
  active_subs_cnt: number;
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
    redemptions: true,
    subscribers: true
  });

  const { data: weeklyData, isLoading } = useQuery({
    queryKey: ['/api/bigquery/account-history', accountId],
    queryFn: () => 
      fetch(`/api/bigquery/account-history/${accountId}`)
        .then(res => {
          if (!res.ok) throw new Error('Failed to fetch account history');
          return res.json();
        }),
    enabled: isOpen && !!accountId,
  });

  const toggleMetric = (metric: keyof typeof visibleMetrics) => {
    setVisibleMetrics(prev => ({
      ...prev,
      [metric]: !prev[metric]
    }));
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatNumber = (value: number) => {
    return new Intl.NumberFormat('en-US').format(value);
  };

  const formatWeekLabel = (weekYr: string) => {
    // Convert "2024W45" to "Week 45, 2024"
    const year = weekYr.substring(0, 4);
    const week = weekYr.substring(5);
    return `Week ${week}, ${year}`;
  };

  // Prepare chart data
  const chartData = weeklyData?.map((item: WeeklyMetric) => ({
    ...item,
    week_label: formatWeekLabel(item.week_yr),
  })) || [];

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="text-xl font-bold">
            Account Details: {accountName}
          </DialogTitle>
          <div className="text-sm text-gray-600">
            12-Week Historical Performance
          </div>
        </DialogHeader>

        {isLoading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin" />
            <span className="ml-2">Loading historical data...</span>
          </div>
        ) : (
          <div className="space-y-6">
            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div className="bg-purple-50 p-4 rounded-lg border">
                <div className="text-sm font-bold text-purple-800 mb-2">Total Spend (12 Weeks)</div>
                <div className="text-lg font-bold text-purple-600">
                  {formatCurrency(chartData.reduce((sum, item) => sum + (item.total_spend || 0), 0))}
                </div>
              </div>
              
              <div className="bg-orange-50 p-4 rounded-lg border">
                <div className="text-sm font-bold text-orange-800 mb-2">Total Texts (12 Weeks)</div>
                <div className="text-lg font-bold text-orange-600">
                  {formatNumber(chartData.reduce((sum, item) => sum + (item.total_texts_delivered || 0), 0))}
                </div>
              </div>
              
              <div className="bg-green-50 p-4 rounded-lg border">
                <div className="text-sm font-bold text-green-800 mb-2">Total Redemptions (12 Weeks)</div>
                <div className="text-lg font-bold text-green-600">
                  {formatNumber(chartData.reduce((sum, item) => sum + (item.coupons_redeemed || 0), 0))}
                </div>
              </div>
              
              <div className="bg-blue-50 p-4 rounded-lg border">
                <div className="text-sm font-bold text-blue-800 mb-2">Avg Subscribers (12 Weeks)</div>
                <div className="text-lg font-bold text-blue-600">
                  {formatNumber(Math.round(chartData.reduce((sum, item) => sum + (item.active_subs_cnt || 0), 0) / Math.max(chartData.length, 1)))}
                </div>
              </div>
            </div>

            {/* Combined Chart with Metric Toggles */}
            <div className="bg-white p-4 rounded-lg border">
              <div className="flex justify-between items-start mb-4">
                <h3 className="text-lg font-semibold">Weekly Performance Trends</h3>
                
                {/* Metric Toggle Controls */}
                <div className="flex flex-wrap gap-4">
                  <div className="flex items-center space-x-2">
                    <Checkbox 
                      id="spend"
                      checked={visibleMetrics.spend}
                      onCheckedChange={() => toggleMetric('spend')}
                    />
                    <label htmlFor="spend" className="text-sm font-medium text-purple-600 cursor-pointer">
                      Spend
                    </label>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <Checkbox 
                      id="texts"
                      checked={visibleMetrics.texts}
                      onCheckedChange={() => toggleMetric('texts')}
                    />
                    <label htmlFor="texts" className="text-sm font-medium text-orange-600 cursor-pointer">
                      Texts
                    </label>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <Checkbox 
                      id="redemptions"
                      checked={visibleMetrics.redemptions}
                      onCheckedChange={() => toggleMetric('redemptions')}
                    />
                    <label htmlFor="redemptions" className="text-sm font-medium text-green-600 cursor-pointer">
                      Redemptions
                    </label>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <Checkbox 
                      id="subscribers"
                      checked={visibleMetrics.subscribers}
                      onCheckedChange={() => toggleMetric('subscribers')}
                    />
                    <label htmlFor="subscribers" className="text-sm font-medium text-blue-600 cursor-pointer">
                      Subscribers
                    </label>
                  </div>
                </div>
              </div>
              
              <ResponsiveContainer width="100%" height={400}>
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="week_label" 
                    angle={-45}
                    textAnchor="end"
                    height={80}
                    fontSize={12}
                  />
                  <YAxis 
                    tickFormatter={(value) => formatNumber(value)}
                    fontSize={12}
                  />
                  <Tooltip 
                    formatter={(value, name) => {
                      const formatValue = name === 'total_spend' ? formatCurrency(Number(value)) : formatNumber(Number(value));
                      const displayName = name === 'total_spend' ? 'Spend' :
                                         name === 'total_texts_delivered' ? 'Texts' :
                                         name === 'coupons_redeemed' ? 'Redemptions' : 'Subscribers';
                      return [formatValue, displayName];
                    }}
                    labelFormatter={(label) => label}
                  />
                  <Legend />
                  
                  {visibleMetrics.spend && (
                    <Line 
                      type="monotone" 
                      dataKey="total_spend" 
                      stroke="#7c3aed" 
                      strokeWidth={3}
                      dot={{ fill: '#7c3aed', strokeWidth: 2, r: 4 }}
                      name="Spend"
                    />
                  )}
                  
                  {visibleMetrics.texts && (
                    <Line 
                      type="monotone" 
                      dataKey="total_texts_delivered" 
                      stroke="#ea580c" 
                      strokeWidth={3}
                      dot={{ fill: '#ea580c', strokeWidth: 2, r: 4 }}
                      name="Texts"
                    />
                  )}
                  
                  {visibleMetrics.redemptions && (
                    <Line 
                      type="monotone" 
                      dataKey="coupons_redeemed" 
                      stroke="#16a34a" 
                      strokeWidth={3}
                      dot={{ fill: '#16a34a', strokeWidth: 2, r: 4 }}
                      name="Redemptions"
                    />
                  )}
                  
                  {visibleMetrics.subscribers && (
                    <Line 
                      type="monotone" 
                      dataKey="active_subs_cnt" 
                      stroke="#2563eb" 
                      strokeWidth={3}
                      dot={{ fill: '#2563eb', strokeWidth: 2, r: 4 }}
                      name="Subscribers"
                    />
                  )}
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Weekly Data Table */}
            <div className="bg-white rounded-lg border">
              <div className="p-4 border-b">
                <h3 className="text-lg font-semibold">Weekly Breakdown</h3>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="p-3 text-left font-semibold">Week</th>
                      <th className="p-3 text-right font-semibold">Spend</th>
                      <th className="p-3 text-right font-semibold">Texts</th>
                      <th className="p-3 text-right font-semibold">Redemptions</th>
                      <th className="p-3 text-right font-semibold">Subscribers</th>
                    </tr>
                  </thead>
                  <tbody>
                    {chartData.map((item, index) => (
                      <tr key={item.week_yr} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-25'}>
                        <td className="p-3 font-medium">{item.week_label}</td>
                        <td className="p-3 text-right font-mono">{formatCurrency(item.total_spend || 0)}</td>
                        <td className="p-3 text-right font-mono">{formatNumber(item.total_texts_delivered || 0)}</td>
                        <td className="p-3 text-right font-mono">{formatNumber(item.coupons_redeemed || 0)}</td>
                        <td className="p-3 text-right font-mono">{formatNumber(item.active_subs_cnt || 0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}