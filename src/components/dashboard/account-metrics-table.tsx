import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { MultiSelect } from "@/components/ui/multi-select";
import { useQuery } from "@tanstack/react-query";
import { useState, useMemo } from "react";
import { ChevronUp, ChevronDown } from "lucide-react";
import AccountDetailModal from './account-detail-modal';

interface AccountMetric {
  account_id: string;
  name: string;
  csm: string;
  status: string;
  total_spend: number;
  total_texts_delivered: number;
  coupons_redeemed: number;
  active_subs_cnt: number;
  location_cnt?: number;
  latest_activity?: string;
  risk_score?: number;
  risk_level?: string;
  status_label?: string;
  // Delta fields for comparison periods
  spend_delta?: number;
  texts_delta?: number;
  coupons_delta?: number;
  subs_delta?: number;
  // Current period fields for comparison views
  current_total_spend?: number;
  current_total_texts_delivered?: number;
  current_coupons_redeemed?: number;
  current_active_subs_cnt?: number;
}

type SortField = 'name' | 'csm' | 'status' | 'total_spend' | 'total_texts_delivered' | 'coupons_redeemed' | 'active_subs_cnt' | 'location_cnt' | 'risk_level' | 'spend_delta' | 'texts_delta' | 'coupons_delta' | 'subs_delta';
type SortDirection = 'asc' | 'desc';
type TimePeriod = 'current_week' | 'previous_week' | 'six_week_average' | 'same_week_last_year' | 'same_week_last_month';

export default function AccountMetricsTable() {
  const [currentPage, setCurrentPage] = useState(1);
  const [sortField, setSortField] = useState<SortField>('name');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');
  const [timePeriod, setTimePeriod] = useState<TimePeriod>('current_week');
  const [selectedCSMs, setSelectedCSMs] = useState<string[]>([]);
  const [selectedRiskLevel, setSelectedRiskLevel] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedStatus, setSelectedStatus] = useState<string>('all');
  const [selectedAccountId, setSelectedAccountId] = useState<string | null>(null);
  const [selectedAccountName, setSelectedAccountName] = useState<string>('');
  const [isModalOpen, setIsModalOpen] = useState(false);
  const accountsPerPage = 25;

  const { data: accountsResponse, isLoading } = useQuery({
    queryKey: ['/api/account-metrics-overview', timePeriod],
    queryFn: () => {
      // Map old period values to new dual-parameter structure
      const comparison = timePeriod === 'current_week' ? null :
        timePeriod === 'previous_week' ? 'vs_previous_wtd' :
        timePeriod === 'six_week_average' ? 'vs_6_week_avg' :
        timePeriod === 'same_week_last_month' ? 'vs_same_wtd_last_month' :
        timePeriod === 'same_week_last_year' ? 'vs_same_wtd_last_year' :
        null;

      const url = comparison
        ? `/api/account-metrics-overview?baseline=current_week&comparison=${comparison}`
        : `/api/account-metrics-overview?baseline=current_week`;

      return fetch(url, {
        headers: {
          'Cache-Control': 'no-cache',
          'Pragma': 'no-cache'
        }
      }).then(res => res.json());
    },
    enabled: true
  });

  // Extract accounts from new response structure - use union dataset when available
  const accounts = accountsResponse?.accounts || accountsResponse?.baseline?.accounts || [];


  const handleSort = (field: SortField) => {
    if (field === sortField) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
    setCurrentPage(1); // Reset to first page when sorting
  };

  const handleTimePeriodChange = (value: TimePeriod) => {
    setTimePeriod(value);
    setCurrentPage(1); // Reset to first page when changing time period
  };

  const getTimePeriodLabel = (period: TimePeriod) => {
    switch (period) {
      case 'current_week': return 'Current Week to Date (through previous complete day)';
      case 'previous_week': return 'Previous WTD (same timeframe)';
      case 'six_week_average': return 'Previous 6 Week Average (same timeframe)';
      case 'same_week_last_year': return 'Same WTD Last Year (same timeframe)';
      case 'same_week_last_month': return 'Same WTD Last Month (same timeframe)';
      default: return 'Current Week to Date (through previous complete day)';
    }
  };

  const handleAccountClick = (account: AccountMetric) => {
    setSelectedAccountId(account.account_id);
    setSelectedAccountName(account.name);
    setIsModalOpen(true);
  };

  const handleModalClose = () => {
    setIsModalOpen(false);
    setSelectedAccountId(null);
    setSelectedAccountName('');
  };

  const { sortedAccounts, paginatedAccounts, totalPages, summaryStats, currentWeekStats, calculatedDeltas, uniqueCSMs, uniqueRiskLevels, uniqueStatuses } = useMemo(() => {
    if (!accounts || !Array.isArray(accounts) || accounts.length === 0) {
      return { 
        sortedAccounts: [], 
        paginatedAccounts: [], 
        totalPages: 0,
        uniqueCSMs: [],
        uniqueRiskLevels: [],
        uniqueStatuses: [],
        summaryStats: {
          totalAccounts: 0,
          highRiskCount: 0,
          medRiskCount: 0,
          lowRiskCount: 0,
          totalSpend: 0,
          totalRedemptions: 0,
          totalTexts: 0,
          totalSubscribers: 0,
        },
        currentWeekStats: {
          totalSpend: 0,
          totalRedemptions: 0,
          totalTexts: 0,
          totalSubscribers: 0,
        },
        calculatedDeltas: {
          spendDelta: 0,
          redemptionsDelta: 0,
          textsDelta: 0,
          subscribersDelta: 0,
        }
      };
    }

    // Get unique CSMs, risk levels, and statuses for filter options
    const uniqueCSMs = Array.from(new Set(accounts.map(acc => acc.csm).filter(Boolean))).sort();
    const uniqueRiskLevels = Array.from(new Set(accounts.map(acc => acc.risk_level).filter(Boolean))).sort();
    const uniqueStatuses = Array.from(new Set(accounts.map(acc => acc.status).filter(Boolean))).sort();

    // Filter accounts based on search, status, CSMs, and risk level
    let filteredAccounts = accounts;
    
    // Search filter (search in account names)
    if (searchQuery.trim()) {
      filteredAccounts = filteredAccounts.filter(acc => 
        acc.name.toLowerCase().includes(searchQuery.trim().toLowerCase())
      );
    }
    
    // Status filter
    if (selectedStatus !== 'all') {
      filteredAccounts = filteredAccounts.filter(acc => acc.status === selectedStatus);
    }
    
    // CSM filter
    if (selectedCSMs.length > 0) {
      filteredAccounts = filteredAccounts.filter(acc => selectedCSMs.includes(acc.csm));
    }
    
    // Risk level filter
    if (selectedRiskLevel !== 'all') {
      filteredAccounts = filteredAccounts.filter(acc => acc.risk_level === selectedRiskLevel);
    }

    // Calculate summary statistics for filtered data (comparison period)
    const summaryStats = filteredAccounts.reduce((acc, account) => {
      acc.totalAccounts += 1;
      acc.totalSpend += account.total_spend || 0;
      acc.totalRedemptions += account.coupons_redeemed || 0;
      acc.totalTexts += account.total_texts_delivered || 0;
      acc.totalSubscribers += account.active_subs_cnt || 0;
      
      // Count risk levels
      const riskLevel = account.risk_level || 'low';
      if (riskLevel === 'high') acc.highRiskCount += 1;
      else if (riskLevel === 'medium') acc.medRiskCount += 1;
      else acc.lowRiskCount += 1;
      
      return acc;
    }, {
      totalAccounts: 0,
      highRiskCount: 0,
      medRiskCount: 0,
      lowRiskCount: 0,
      totalSpend: 0,
      totalRedemptions: 0,
      totalTexts: 0,
      totalSubscribers: 0,
    });

    // Use backend-provided metrics for summary cards
    let currentWeekStats = { ...summaryStats };
    let calculatedDeltas = {
      spendDelta: 0,
      redemptionsDelta: 0,
      textsDelta: 0,
      subscribersDelta: 0,
    };

    if (timePeriod !== 'current_week' && accountsResponse?.baseline && accountsResponse?.comparison) {
      // Use pre-calculated metrics from backend API response
      currentWeekStats = {
        totalAccounts: summaryStats.totalAccounts,
        highRiskCount: summaryStats.highRiskCount,
        medRiskCount: summaryStats.medRiskCount,
        lowRiskCount: summaryStats.lowRiskCount,
        totalSpend: accountsResponse.baseline.metrics.total_spend,
        totalTexts: accountsResponse.baseline.metrics.total_texts,
        totalRedemptions: accountsResponse.baseline.metrics.total_redemptions,
        totalSubscribers: accountsResponse.baseline.metrics.total_subscribers,
      };

      // Calculate deltas from backend metrics
      calculatedDeltas = {
        spendDelta: accountsResponse.baseline.metrics.total_spend - accountsResponse.comparison.metrics.total_spend,
        textsDelta: accountsResponse.baseline.metrics.total_texts - accountsResponse.comparison.metrics.total_texts,
        redemptionsDelta: accountsResponse.baseline.metrics.total_redemptions - accountsResponse.comparison.metrics.total_redemptions,
        subscribersDelta: accountsResponse.baseline.metrics.total_subscribers - accountsResponse.comparison.metrics.total_subscribers,
      };
    }


    // Sort filtered accounts
    const sortedAccounts = [...filteredAccounts].sort((a, b) => {
      let aValue: any, bValue: any;
      
      // Handle delta fields - they're already properly named in the data
      if (sortField === 'spend_delta' || sortField === 'texts_delta' || 
          sortField === 'coupons_delta' || sortField === 'subs_delta') {
        aValue = a[sortField];
        bValue = b[sortField];
      } else {
        aValue = a[sortField];
        bValue = b[sortField];
      }

      // Handle null/undefined values - for delta fields, treat as 0
      if (sortField.endsWith('_delta')) {
        aValue = aValue ?? 0;
        bValue = bValue ?? 0;
      } else {
        if (aValue == null) aValue = '';
        if (bValue == null) bValue = '';
      }

      // Special handling for risk_level sorting by priority
      if (sortField === 'risk_level') {
        const riskPriority = { 'high': 3, 'medium': 2, 'low': 1 };
        const aRisk = String(aValue).toLowerCase();
        const bRisk = String(bValue).toLowerCase();
        const aPriority = riskPriority[aRisk] || 0;
        const bPriority = riskPriority[bRisk] || 0;
        
        return sortDirection === 'asc' ? aPriority - bPriority : bPriority - aPriority;
      }

      // Handle numeric fields (including delta fields)
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortDirection === 'asc' ? aValue - bValue : bValue - aValue;
      }

      // Handle string fields
      const aStr = String(aValue).toLowerCase();
      const bStr = String(bValue).toLowerCase();
      
      if (sortDirection === 'asc') {
        return aStr.localeCompare(bStr);
      } else {
        return bStr.localeCompare(aStr);
      }
    });

    // Paginate sorted accounts
    const startIndex = (currentPage - 1) * accountsPerPage;
    const endIndex = startIndex + accountsPerPage;
    const paginatedAccounts = sortedAccounts.slice(startIndex, endIndex);
    const totalPages = Math.ceil(sortedAccounts.length / accountsPerPage);

    return { sortedAccounts, paginatedAccounts, totalPages, summaryStats, currentWeekStats, calculatedDeltas, uniqueCSMs, uniqueRiskLevels, uniqueStatuses };
  }, [accounts, currentPage, accountsPerPage, sortField, sortDirection, timePeriod, selectedCSMs, selectedRiskLevel, searchQuery, selectedStatus]);

  if (isLoading) {
    return (
      <div className="p-6">
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        </div>
      </div>
    );
  }

  // Debug logging
  console.log('WEEKLY COMPONENT DEBUG:', {
    timePeriod,
    shouldShowDeltas: timePeriod !== 'current_week',
    sampleAccount: accounts?.[0]
  });

  if (!accounts || accounts.length === 0) {
    return (
      <div className="p-6">
        <div className="text-center py-12">
          <p className="text-gray-500">No account data available</p>
        </div>
      </div>
    );
  }

  const formatCurrency = (value: number) => 
    new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value);
  
  const formatCurrencyWhole = (value: number) => 
    new Intl.NumberFormat('en-US', { 
      style: 'currency', 
      currency: 'USD', 
      minimumFractionDigits: 0,
      maximumFractionDigits: 0 
    }).format(value);

  const formatNumber = (value: number) => 
    new Intl.NumberFormat('en-US').format(value);

  const formatDelta = (value: number, type: 'currency' | 'number') => {
    // Handle undefined or null values
    if (value === undefined || value === null) {
      return <span className="font-mono text-sm text-gray-400">—</span>;
    }
    
    const formattedValue = type === 'currency' 
      ? formatCurrencyWhole(Math.abs(value))
      : formatNumber(Math.abs(value));
    
    const sign = value > 0 ? '+' : value < 0 ? '-' : '';
    const colorClass = value > 0 ? 'text-green-600' : value < 0 ? 'text-red-600' : 'text-gray-500';
    
    return (
      <span className={`font-mono text-sm ${colorClass}`}>
        {sign}{formattedValue}
      </span>
    );
  };

  const getRiskBadge = (riskLevel: string) => {
    const variants: Record<string, any> = {
      'high': 'destructive',
      'medium': 'default',
      'low': 'secondary'
    };
    return variants[riskLevel] || 'secondary';
  };

  const getStatusBadge = (status: string) => {
    const variants: Record<string, any> = {
      'ACTIVE': 'default',
      'LAUNCHED': 'default',
      'FROZEN': 'secondary',
      'INACTIVE': 'secondary',
      'CANCELLED': 'destructive'
    };
    return variants[status] || 'secondary';
  };

  const getStatusColor = (status: string) => {
    const colors: Record<string, string> = {
      'FROZEN': 'text-blue-700 bg-blue-50 border-blue-200',
      'LAUNCHED': 'text-green-700 bg-green-50 border-green-200',
      'ACTIVE': 'text-green-700 bg-green-50 border-green-200'
    };
    return colors[status] || '';
  };

  const SortableHeader = ({ field, children, className = "" }: { field: SortField; children: React.ReactNode; className?: string }) => (
    <th 
      className={`p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors ${className}`}
      onClick={() => handleSort(field)}
    >
      <div className="flex items-center gap-1">
        {children}
        <div className="flex flex-col">
          <ChevronUp 
            size={12} 
            className={`${sortField === field && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
          />
          <ChevronDown 
            size={12} 
            className={`${sortField === field && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
          />
        </div>
      </div>
    </th>
  );

  return (
    <>
      <div className="p-6">
        <div className="mb-6">
          <div className="flex justify-between items-start">
            <div>
              <h2 className="font-semibold text-gray-900 text-[24px]">Account Metrics Overview</h2>
              <p className="text-sm text-gray-600">Key performance indicators for all restaurant accounts</p>
            </div>
            <div className="text-sm text-muted-foreground">
              {accounts && accounts.length > 0 && (
                <span>Showing {Math.min(currentPage * accountsPerPage, accounts.length)} of {accounts.length} companies</span>
              )}
            </div>
          </div>
        </div>
          {/* Time Period Selector and Filters */}
          <div className="mb-4 space-y-4">
            <div className="flex justify-between items-center">
              <div className="flex items-center gap-3">
                <label className="text-sm font-medium text-gray-700">Time Period:</label>
                <Select value={timePeriod} onValueChange={handleTimePeriodChange}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Select time period" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="current_week">Current WTD</SelectItem>
                    <SelectItem value="previous_week">vs. Previous WTD</SelectItem>
                    <SelectItem value="six_week_average">vs. Previous 6 Week Avg.</SelectItem>
                    <SelectItem value="same_week_last_month">vs. Same WTD Last Month</SelectItem>
                    <SelectItem value="same_week_last_year">vs. Same WTD Last Year</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="text-sm text-gray-600">
                Showing data for: <span className="font-medium text-gray-800">{getTimePeriodLabel(timePeriod)}</span>
              </div>
            </div>
            
            {/* Filters Bar */}
            <div className="flex items-center gap-4 mb-4">
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium text-gray-700">Search:</label>
                <Input
                  type="text"
                  placeholder="Search account names..."
                  value={searchQuery}
                  onChange={(e) => { setSearchQuery(e.target.value); setCurrentPage(1); }}
                  className="w-48"
                />
              </div>
              
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium text-gray-700">Status:</label>
                <Select value={selectedStatus} onValueChange={(value) => { setSelectedStatus(value); setCurrentPage(1); }}>
                  <SelectTrigger className="w-40">
                    <SelectValue placeholder="All Statuses" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Statuses</SelectItem>
                    {uniqueStatuses.map((status) => (
                      <SelectItem key={status} value={status}>{status}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium text-gray-700">CSM:</label>
                <MultiSelect
                  options={uniqueCSMs}
                  value={selectedCSMs}
                  onChange={(value) => { setSelectedCSMs(value); setCurrentPage(1); }}
                  placeholder="All CSMs"
                />
              </div>
              
              <div className="text-sm text-gray-600 ml-auto">
                {accounts && (
                  <span>Showing {paginatedAccounts.length} of {sortedAccounts.length} accounts ({accounts.length} total)</span>
                )}
              </div>
            </div>
          </div>

          {/* Summary Section */}
          <div className={`mb-6 grid gap-4 w-full ${timePeriod === 'current_week' ? 'grid-cols-1 md:grid-cols-2 lg:grid-cols-4 xl:grid-cols-4' : 'grid-cols-1 sm:grid-cols-2 md:grid-cols-4'}`}>
            {/* Total Spend */}
            <div className="bg-purple-50 p-4 rounded-lg border">
              <div className="text-sm font-bold text-purple-800 mb-3">Total Spend</div>
              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <span className="text-xs text-gray-600">Filtered Accounts</span>
                  <span className="text-sm font-bold text-purple-600">{formatCurrencyWhole(summaryStats.totalSpend)}</span>
                </div>
                {timePeriod !== 'current_week' && (
                  <>
                    <div className="flex justify-between items-center">
                      <span className="text-xs text-gray-600">Comparison</span>
                      <span className="text-sm font-bold text-gray-600">{formatCurrencyWhole(currentWeekStats.totalSpend - calculatedDeltas.spendDelta)}</span>
                    </div>
                    <div className="flex justify-between items-center pt-1 border-t">
                      <span className="text-xs text-gray-600">Delta</span>
                      <div className="text-sm font-bold">
                        {formatDelta(calculatedDeltas.spendDelta, 'currency')}
                      </div>
                    </div>
                  </>
                )}
              </div>
            </div>
            
            {/* Total Texts */}
            <div className="bg-orange-50 p-4 rounded-lg border">
              <div className="text-sm font-bold text-orange-800 mb-3">Total Texts</div>
              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <span className="text-xs text-gray-600">Filtered Accounts</span>
                  <span className="text-sm font-bold text-orange-600">{summaryStats.totalTexts.toLocaleString()}</span>
                </div>
                {timePeriod !== 'current_week' && (
                  <>
                    <div className="flex justify-between items-center">
                      <span className="text-xs text-gray-600">Comparison</span>
                      <span className="text-sm font-bold text-gray-600">{(currentWeekStats.totalTexts - calculatedDeltas.textsDelta).toLocaleString()}</span>
                    </div>
                    <div className="flex justify-between items-center pt-1 border-t">
                      <span className="text-xs text-gray-600">Delta</span>
                      <div className="text-sm font-bold">
                        {formatDelta(calculatedDeltas.textsDelta, 'number')}
                      </div>
                    </div>
                  </>
                )}
              </div>
            </div>
            
            {/* Total Redemptions */}
            <div className="bg-green-50 p-4 rounded-lg border">
              <div className="text-sm font-bold text-green-800 mb-3">Total Redemptions</div>
              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <span className="text-xs text-gray-600">Filtered Accounts</span>
                  <span className="text-sm font-bold text-green-600">{summaryStats.totalRedemptions.toLocaleString()}</span>
                </div>
                {timePeriod !== 'current_week' && (
                  <>
                    <div className="flex justify-between items-center">
                      <span className="text-xs text-gray-600">Comparison</span>
                      <span className="text-sm font-bold text-gray-600">{(currentWeekStats.totalRedemptions - calculatedDeltas.redemptionsDelta).toLocaleString()}</span>
                    </div>
                    <div className="flex justify-between items-center pt-1 border-t">
                      <span className="text-xs text-gray-600">Delta</span>
                      <div className="text-sm font-bold">
                        {formatDelta(calculatedDeltas.redemptionsDelta, 'number')}
                      </div>
                    </div>
                  </>
                )}
              </div>
            </div>
            
            {/* Total Subscribers */}
            <div className="bg-blue-50 p-4 rounded-lg border">
              <div className="text-sm font-bold text-blue-800 mb-3">Total Subscribers</div>
              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <span className="text-xs text-gray-600">Filtered Accounts</span>
                  <span className="text-sm font-bold text-blue-600">{summaryStats.totalSubscribers.toLocaleString()}</span>
                </div>
                {timePeriod !== 'current_week' && (
                  <>
                    <div className="flex justify-between items-center">
                      <span className="text-xs text-gray-600">Comparison</span>
                      <span className="text-sm font-bold text-gray-600">{(currentWeekStats.totalSubscribers - calculatedDeltas.subscribersDelta).toLocaleString()}</span>
                    </div>
                    <div className="flex justify-between items-center pt-1 border-t">
                      <span className="text-xs text-gray-600">Delta</span>
                      <div className="text-sm font-bold">
                        {formatDelta(calculatedDeltas.subscribersDelta, 'number')}
                      </div>
                    </div>
                  </>
                )}
              </div>
            </div>
            

          </div>

          <div className="overflow-x-auto">
            <div className="max-h-[600px] overflow-y-auto border rounded-lg">
              <table className="w-full border-collapse">
                <thead className="sticky top-0 bg-gray-50 z-10">
                  <tr className="border-b">
                    <SortableHeader field="name" className="text-left">Account</SortableHeader>
                    <SortableHeader field="csm" className="text-left">CSM</SortableHeader>
                    <SortableHeader field="status" className="text-left">Status</SortableHeader>
                    {timePeriod !== 'current_week' && (
                      <th className="p-3 font-medium text-left">Change Label</th>
                    )}
                    <SortableHeader field="total_spend" className="text-right">Total Spend</SortableHeader>
                    {timePeriod !== 'current_week' && (
                      <SortableHeader field="spend_delta" className="text-right">Spend Δ</SortableHeader>
                    )}
                    <SortableHeader field="total_texts_delivered" className="text-right">Texts Delivered</SortableHeader>
                    {timePeriod !== 'current_week' && (
                      <SortableHeader field="texts_delta" className="text-right">Texts Δ</SortableHeader>
                    )}
                    <SortableHeader field="coupons_redeemed" className="text-right">Redemptions</SortableHeader>
                    {timePeriod !== 'current_week' && (
                      <SortableHeader field="coupons_delta" className="text-right">Coupons Δ</SortableHeader>
                    )}
                    <SortableHeader field="active_subs_cnt" className="text-right">Subscribers</SortableHeader>
                    {timePeriod !== 'current_week' && (
                      <SortableHeader field="subs_delta" className="text-right">Subs Δ</SortableHeader>
                    )}
                  </tr>
                </thead>
                <tbody>
                  {paginatedAccounts.map((account: AccountMetric, index: number) => (
                    <tr 
                      key={`${account.account_id}-${index}`} 
                      className={`border-b hover:bg-blue-50 cursor-pointer transition-colors ${index % 2 === 0 ? 'bg-white' : 'bg-gray-25'}`}
                      onClick={() => handleAccountClick(account)}
                    >
                      <td className="p-3">
                        <div>
                          <div className="font-medium text-sm">{account.name}</div>
                          <div className="text-xs text-gray-500">{account.account_id}</div>
                        </div>
                      </td>
                      <td className="p-3 text-sm">{account.csm || 'Unassigned'}</td>
                      <td className="p-3">
                        <Badge variant={getStatusBadge(account.status)} className={`text-xs ${getStatusColor(account.status)}`}>
                          {account.status}
                        </Badge>
                      </td>
                      {timePeriod !== 'current_week' && (
                        <td className="p-3">
                          {account.status_label && (
                            <Badge variant="outline" className="text-xs">
                              {account.status_label}
                            </Badge>
                          )}
                        </td>
                      )}
                      <td className="p-3 text-right font-mono text-sm">
                        {formatCurrencyWhole(account.total_spend)}
                      </td>
                      {timePeriod !== 'current_week' && (
                        <td className="p-3 text-right font-mono text-sm">
                          {formatDelta(account.spend_delta || 0, 'currency')}
                        </td>
                      )}
                      <td className="p-3 text-right font-mono text-sm">
                        {formatNumber(account.total_texts_delivered)}
                      </td>
                      {timePeriod !== 'current_week' && (
                        <td className="p-3 text-right font-mono text-sm">
                          {formatDelta(account.texts_delta || 0, 'number')}
                        </td>
                      )}
                      <td className="p-3 text-right font-mono text-sm">
                        {formatNumber(account.coupons_redeemed)}
                      </td>
                      {timePeriod !== 'current_week' && (
                        <td className="p-3 text-right font-mono text-sm">
                          {formatDelta(account.coupons_delta || 0, 'number')}
                        </td>
                      )}
                      <td className="p-3 text-right font-mono text-sm">
                        {formatNumber(account.active_subs_cnt)}
                      </td>
                      {timePeriod !== 'current_week' && (
                        <td className="p-3 text-right font-mono text-sm">
                          {formatDelta(account.subs_delta || 0, 'number')}
                        </td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
          
          <div className="mt-4 flex justify-between items-center">
            <div className="text-sm text-gray-600">
              Showing {paginatedAccounts.length} of {sortedAccounts.length} accounts (Page {currentPage} of {totalPages})
              {sortField && (
                <span className="ml-2 text-blue-600">
                  • Sorted by {sortField.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())} ({sortDirection === 'asc' ? 'ascending' : 'descending'})
                </span>
              )}
            </div>
            
            <div className="flex gap-2">
              <button 
                onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
                disabled={currentPage === 1}
                className="px-3 py-1 text-sm border rounded disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
              >
                Previous
              </button>
              <span className="px-3 py-1 text-sm border rounded bg-blue-50 text-blue-700">
                {currentPage}
              </span>
              <button 
                onClick={() => setCurrentPage(Math.min(totalPages, currentPage + 1))}
                disabled={currentPage === totalPages}
                className="px-3 py-1 text-sm border rounded disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
              >
                Next
              </button>
            </div>
          </div>
        </div>
      <AccountDetailModal
        accountId={selectedAccountId}
        accountName={selectedAccountName}
        isOpen={isModalOpen}
        onClose={handleModalClose}
      />
    </>
  );
}