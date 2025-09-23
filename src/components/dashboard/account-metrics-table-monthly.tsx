import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { MultiSelect } from "@/components/ui/multi-select";
import { useQuery } from "@tanstack/react-query";
import { useState, useMemo } from "react";
import { ChevronUp, ChevronDown } from "lucide-react";
import AccountDetailModal from './account-detail-modal-monthly';

interface AccountMetric {
  account_id: string;
  name: string;
  csm: string;
  status: string;
  total_spend: number;
  total_texts_delivered: number;
  coupons_redeemed: number;
  active_subs_cnt: number;
  location_cnt: number;
  latest_activity: string;
  risk_score?: number;
  risk_level?: string;
  risk_reasons?: string[]; // Database-provided historical risk reasons (from previous completed month)
  trending_risk_level?: string;
  trending_risk_reasons?: string[]; // Database-provided trending risk reasons (from current month)
  risk_flags?: {
    monthlyRedemptionsFlag: boolean;
    lowActivityFlag: boolean;
    spendDropFlag: boolean;
    redemptionsDropFlag: boolean;
  };
  trending_risk_flags?: {
    monthlyRedemptionsFlag: boolean;
    lowActivityFlag: boolean;
    spendDropFlag: boolean;
    redemptionsDropFlag: boolean;
  };
  spend_delta: number;
  texts_delta: number;
  coupons_delta: number;
  subs_delta: number;
  status_label?: string;
}

type SortField = 'name' | 'csm' | 'status' | 'total_spend' | 'total_texts_delivered' | 'coupons_redeemed' | 'active_subs_cnt' | 'location_cnt' | 'risk_level' | 'trending_risk_level' | 'spend_delta' | 'texts_delta' | 'coupons_delta' | 'subs_delta';
type SortDirection = 'asc' | 'desc';
type TimePeriod = 'current_month' | 'previous_month' | 'last_3_month_avg' | 'this_month_last_year';

// Helper function to format risk flags into readable text
const formatRiskFlags = (flags?: { monthlyRedemptionsFlag: boolean; lowActivityFlag: boolean; spendDropFlag: boolean; redemptionsDropFlag: boolean }) => {
  if (!flags) return 'No flags';
  
  const activeFlags = [];
  if (flags.monthlyRedemptionsFlag) activeFlags.push('Low Monthly Redemptions');
  if (flags.lowActivityFlag) activeFlags.push('Low Activity');
  if (flags.spendDropFlag) activeFlags.push('Spend Drop');
  if (flags.redemptionsDropFlag) activeFlags.push('Redemptions Drop');
  
  return activeFlags.length > 0 ? activeFlags.join(', ') : 'No flags';
};

// Helper function to extract individual risk reasons for filtering
const getRiskReasons = (account: AccountMetric, isForTrending: boolean = false): string[] => {
  // Use database-provided risk reasons when available and non-empty
  if (isForTrending && account.trending_risk_reasons && account.trending_risk_reasons.length > 0) {
    return account.trending_risk_reasons;
  } else if (!isForTrending && account.risk_reasons && account.risk_reasons.length > 0) {
    return account.risk_reasons;
  }
  
  // Fallback to old flag-based calculation for comparison periods
  const flags = isForTrending ? account.trending_risk_flags : account.risk_flags;
  const reasons: string[] = [];
  
  // Handle FROZEN accounts specially
  if (account.status === 'FROZEN') {
    reasons.push('Frozen Account Status');
    return reasons;
  }
  
  // Handle regular flag-based reasons
  if (flags?.monthlyRedemptionsFlag) reasons.push('Low Monthly Redemptions');
  if (flags?.lowActivityFlag) reasons.push('Low Activity');  
  if (flags?.spendDropFlag) reasons.push('Spend Drop');
  if (flags?.redemptionsDropFlag) reasons.push('Redemptions Drop');
  
  return reasons.length > 0 ? reasons : ['No flags'];
};

export default function AccountMetricsTableMonthly() {
  const [currentPage, setCurrentPage] = useState(1);
  const [sortField, setSortField] = useState<SortField>('name');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');
  const [timePeriod, setTimePeriod] = useState<TimePeriod>('current_month');
  const [selectedCSMs, setSelectedCSMs] = useState<string[]>([]);
  const [selectedRiskLevel, setSelectedRiskLevel] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedStatus, setSelectedStatus] = useState<string>('all');
  const [selectedTrendingRiskLevel, setSelectedTrendingRiskLevel] = useState<string>('all');
  const [selectedRiskReasons, setSelectedRiskReasons] = useState<string[]>([]);
  const [selectedTrendingRiskReasons, setSelectedTrendingRiskReasons] = useState<string[]>([]);
  const [selectedAccountId, setSelectedAccountId] = useState<string | null>(null);
  const [selectedAccountName, setSelectedAccountName] = useState<string>('');
  const [isModalOpen, setIsModalOpen] = useState(false);
  const accountsPerPage = 25;

  // Map old period values to new comparison values
  const getComparisonParam = (period: string) => {
    switch (period) {
      case 'previous_month': return 'vs_previous_month';
      case 'last_3_month_avg': return 'vs_3_month_avg';
      case 'this_month_last_year': return 'vs_same_month_last_year';
      case 'current_month':
      default: return undefined; // No comparison for current month baseline
    }
  };

  const { data: apiResponse, isLoading } = useQuery({
    queryKey: ['/api/account-metrics-monthly', timePeriod],
    queryFn: async () => {
      const comparison = getComparisonParam(timePeriod);
      const params = new URLSearchParams();
      if (comparison) {
        params.append('comparison', comparison);
      }
      const url = `/api/account-metrics-monthly${params.toString() ? '?' + params.toString() : ''}`;
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error('Failed to fetch monthly accounts');
      }
      return response.json();
    },
    enabled: true,
    staleTime: 30000,
    gcTime: 300000
  });

  // Extract accounts from new response structure - use union dataset when available (same pattern as weekly view)
  const accounts = apiResponse?.accounts || apiResponse?.baseline?.accounts || [];
  const riskCounts = apiResponse?.risk_counts || { trending: { high: 0, medium: 0, low: 0 }, historical: { high: 0, medium: 0, low: 0 } };

  // Extract metrics from API response (same pattern as weekly view)
  const baselineMetrics = apiResponse?.baseline?.metrics || { total_spend: 0, total_texts: 0, total_redemptions: 0, total_subscribers: 0 };
  const comparisonMetrics = apiResponse?.comparison?.metrics || null;

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
      case 'current_month': return 'Current Month to Date (through previous complete day)';
      case 'previous_month': return 'Previous Month MTD (same timeframe)';
      case 'last_3_month_avg': return 'Last 3 Month Average (same timeframe)';
      case 'this_month_last_year': return 'Same Month Last Year (same timeframe)';
      default: return 'Current Month to Date (through previous complete day)';
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


  const { sortedAccounts, paginatedAccounts, totalPages, summaryStats, uniqueCSMs, uniqueRiskLevels, uniqueStatuses, uniqueTrendingRiskLevels, uniqueRiskReasons, uniqueTrendingRiskReasons } = useMemo(() => {
    if (!accounts || !Array.isArray(accounts) || accounts.length === 0) {
      return { 
        sortedAccounts: [], 
        paginatedAccounts: [], 
        totalPages: 0,
        uniqueCSMs: [],
        uniqueRiskLevels: [],
        uniqueStatuses: [],
        uniqueTrendingRiskLevels: [],
        uniqueRiskReasons: [],
        uniqueTrendingRiskReasons: [],
        summaryStats: {
          totalAccounts: 0,
          highRiskCount: 0,
          medRiskCount: 0,
          lowRiskCount: 0,
          trendingHighRiskCount: 0,
          trendingMedRiskCount: 0,
          trendingLowRiskCount: 0,
          totalSpend: 0,
          totalRedemptions: 0,
          totalTexts: 0,
          totalSubscribers: 0,
          highRiskDelta: 0,
          medRiskDelta: 0,
          lowRiskDelta: 0
        },
      };
    }


    // Add trending risk level and flags (deltas already provided by server)
    const accountsWithTrending = accounts.map(account => {
      // For FROZEN accounts, use the risk data from the API which is already correctly calculated
      if (account.status === 'FROZEN') {
        return {
          ...account,
          trending_risk_level: account.trending_risk_level || account.riskLevel,
          trending_risk_flags: null, // FROZEN accounts don't use traditional flag system
        };
      }

      // Use database-provided trending risk data (calculated by ETL) and server-provided deltas
      return {
        ...account,
      };
    });

    // Get unique values for all filter options
    const uniqueCSMs = Array.from(new Set(accountsWithTrending.map(acc => acc.csm).filter(Boolean))).sort();

    // Custom sort for risk levels by severity (high, medium, low)
    const riskLevelOrder = ['high', 'medium', 'low'];
    const sortRiskLevels = (levels: string[]) => {
      return levels.sort((a, b) => {
        const aIndex = riskLevelOrder.indexOf(a.toLowerCase());
        const bIndex = riskLevelOrder.indexOf(b.toLowerCase());
        return aIndex - bIndex;
      });
    };

    const uniqueRiskLevels = sortRiskLevels(Array.from(new Set(accountsWithTrending.map(acc => acc.riskLevel || acc.risk_level).filter(Boolean))));
    const uniqueStatuses = Array.from(new Set(accountsWithTrending.map(acc => acc.status).filter(Boolean))).sort();
    const uniqueTrendingRiskLevels = sortRiskLevels(Array.from(new Set(accountsWithTrending.map(acc => acc.trending_risk_level).filter(Boolean))));
    
    // Get unique risk reasons from all accounts
    const allRiskReasons = new Set<string>();
    const allTrendingRiskReasons = new Set<string>();
    
    accountsWithTrending.forEach(acc => {
      getRiskReasons(acc, false).forEach(reason => allRiskReasons.add(reason));
      getRiskReasons(acc, true).forEach(reason => allTrendingRiskReasons.add(reason));
    });
    
    // Custom sort order for risk reasons
    const riskReasonOrder = [
      'Recently Archived', // Archived flag first
      'Frozen Account Status', 'Frozen & Inactive', // Frozen options
      'Low Activity', 'Low Engagement Combo', 'Low Monthly Redemptions', 'Redemptions Drop', 'Spend Drop', // Launched/active options alphabetically  
      'No flags' // Bottom
    ];
    
    const sortRiskReasons = (reasons: string[]) => {
      return reasons.sort((a, b) => {
        const aIndex = riskReasonOrder.indexOf(a);
        const bIndex = riskReasonOrder.indexOf(b);
        // If not found in order array, put at end (but before "No flags")
        const aPos = aIndex === -1 ? riskReasonOrder.length - 1 : aIndex;
        const bPos = bIndex === -1 ? riskReasonOrder.length - 1 : bIndex;
        return aPos - bPos;
      });
    };
    
    const uniqueRiskReasons = sortRiskReasons(Array.from(allRiskReasons));
    const uniqueTrendingRiskReasons = sortRiskReasons(Array.from(allTrendingRiskReasons));

    // Filter accounts based on search, status, CSMs, and risk level
    let filteredAccounts = accountsWithTrending;
    
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
      filteredAccounts = filteredAccounts.filter(acc => (acc.riskLevel || acc.risk_level) === selectedRiskLevel);
    }
    
    // Trending risk level filter
    if (selectedTrendingRiskLevel !== 'all') {
      filteredAccounts = filteredAccounts.filter(acc => acc.trending_risk_level === selectedTrendingRiskLevel);
    }
    
    // Risk reasons filter
    if (selectedRiskReasons.length > 0) {
      filteredAccounts = filteredAccounts.filter(acc => {
        const accountRiskReasons = getRiskReasons(acc, false);
        return selectedRiskReasons.some(reason => accountRiskReasons.includes(reason));
      });
    }
    
    // Trending risk reasons filter
    if (selectedTrendingRiskReasons.length > 0) {
      filteredAccounts = filteredAccounts.filter(acc => {
        const accountTrendingRiskReasons = getRiskReasons(acc, true);
        return selectedTrendingRiskReasons.some(reason => accountTrendingRiskReasons.includes(reason));
      });
    }

    // Calculate summary statistics for filtered data
    const summaryStats = filteredAccounts.reduce((acc, account) => {
      acc.totalAccounts += 1;
      acc.totalSpend += parseFloat(account.total_spend) || 0;
      acc.totalRedemptions += parseInt(account.coupons_redeemed) || 0;
      acc.totalTexts += parseInt(account.total_texts_delivered) || 0;
      acc.totalSubscribers += parseInt(account.active_subs_cnt) || 0;
      
      // Count risk levels (current month)
      const riskLevel = account.riskLevel || account.risk_level || 'low';
      if (riskLevel === 'high') acc.highRiskCount += 1;
      else if (riskLevel === 'medium') acc.medRiskCount += 1;
      else acc.lowRiskCount += 1;

      // Count trending risk levels
      const trendingRiskLevel = account.trending_risk_level || 'low';
      if (trendingRiskLevel === 'high') acc.trendingHighRiskCount += 1;
      else if (trendingRiskLevel === 'medium') acc.trendingMedRiskCount += 1;
      else acc.trendingLowRiskCount += 1;

      return acc;
    }, {
      totalAccounts: 0,
      highRiskCount: 0,
      medRiskCount: 0,
      lowRiskCount: 0,
      trendingHighRiskCount: 0,
      trendingMedRiskCount: 0,
      trendingLowRiskCount: 0,
      totalSpend: 0,
      totalRedemptions: 0,
      totalTexts: 0,
      totalSubscribers: 0,
      highRiskDelta: 0,
      medRiskDelta: 0,
      lowRiskDelta: 0
    });



    // Sort accounts
    const sortedAccounts = [...filteredAccounts].sort((a, b) => {
      let aValue: any, bValue: any;
      
      // Handle delta fields - they're now properly added to account objects
      if (sortField === 'spend_delta' || sortField === 'texts_delta' || 
          sortField === 'coupons_delta' || sortField === 'subs_delta') {
        aValue = a[sortField];
        bValue = b[sortField];
      } else if (sortField === 'risk_level') {
        // Handle the risk_level field specifically since it might be stored as either 'riskLevel' or 'risk_level'
        aValue = a.riskLevel || a.risk_level;
        bValue = b.riskLevel || b.risk_level;
        
        // Special handling for risk_level sorting by priority
        const riskPriority = { 'high': 3, 'medium': 2, 'low': 1 };
        const aRisk = String(aValue || '').toLowerCase();
        const bRisk = String(bValue || '').toLowerCase();
        const aPriority = riskPriority[aRisk] || 0;
        const bPriority = riskPriority[bRisk] || 0;
        
        return sortDirection === 'asc' ? aPriority - bPriority : bPriority - aPriority;
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
      
      // Handle numeric fields (including delta fields)
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortDirection === 'asc' ? aValue - bValue : bValue - aValue;
      }
      
      // Handle string fields
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        return sortDirection === 'asc' 
          ? aValue.localeCompare(bValue)
          : bValue.localeCompare(aValue);
      }
      
      // Fallback to numeric comparison
      const aNum = Number(aValue) || 0;
      const bNum = Number(bValue) || 0;
      
      return sortDirection === 'asc' ? aNum - bNum : bNum - aNum;
    });

    // Paginate
    const startIndex = (currentPage - 1) * accountsPerPage;
    const paginatedAccounts = sortedAccounts.slice(startIndex, startIndex + accountsPerPage);
    const totalPages = Math.ceil(sortedAccounts.length / accountsPerPage);

    return { sortedAccounts, paginatedAccounts, totalPages, summaryStats, uniqueCSMs, uniqueRiskLevels, uniqueStatuses, uniqueTrendingRiskLevels, uniqueRiskReasons, uniqueTrendingRiskReasons };
  }, [accounts, selectedCSMs, selectedRiskLevel, searchQuery, selectedStatus, selectedTrendingRiskLevel, selectedRiskReasons, selectedTrendingRiskReasons, sortField, sortDirection, currentPage, timePeriod]);

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

  const formatDelta = (value: number, type: 'currency' | 'number') => {
    // Handle undefined or null values
    if (value === undefined || value === null) {
      return <span className="font-mono text-sm text-gray-400">—</span>;
    }
    
    const formattedValue = type === 'currency' 
      ? formatCurrency(Math.abs(value))
      : formatNumber(Math.abs(value));
    
    const sign = value > 0 ? '+' : value < 0 ? '-' : '';
    const colorClass = value > 0 ? 'text-green-600' : value < 0 ? 'text-red-600' : 'text-gray-500';
    
    return (
      <span className={`font-mono text-sm ${colorClass}`}>
        {sign}{formattedValue}
      </span>
    );
  };

  const formatCurrencyWhole = (value: number) => 
    new Intl.NumberFormat('en-US', { 
      style: 'currency', 
      currency: 'USD', 
      minimumFractionDigits: 0,
      maximumFractionDigits: 0 
    }).format(value);

  const getRiskBadgeColor = (riskLevel: string) => {
    switch (riskLevel) {
      case 'high': return 'bg-red-100 text-red-800';
      case 'medium': return 'bg-yellow-100 text-yellow-800';
      case 'low': return 'bg-green-100 text-green-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  if (isLoading) {
    return (
      <div className="p-6">
        <div className="text-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-purple-600 mx-auto"></div>
          <p className="mt-2 text-gray-500">Loading monthly data...</p>
        </div>
      </div>
    );
  }

  return (
    <>
    <div className="p-6">
      <div className="mb-6">
        <div className="flex justify-between items-start">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Account Metrics Overview</h2>
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
                  <SelectItem value="current_month">Current MTD</SelectItem>
                  <SelectItem value="previous_month">vs. Previous Month MTD</SelectItem>
                  <SelectItem value="last_3_month_avg">vs. Last 3 Month Avg</SelectItem>
                  <SelectItem value="this_month_last_year">vs. Same Month Last Year</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="text-sm text-gray-600">
              Showing data for: <span className="font-medium text-gray-800">{getTimePeriodLabel(timePeriod)}</span>
            </div>
          </div>

          {/* Filters */}
          <div className="flex flex-wrap items-center gap-4 mb-4">
            {/* Search - Always first */}
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

            {/* CSM - Always second */}
            <div className="flex items-center gap-2">
              <label className="text-sm font-medium text-gray-700">CSM:</label>
              <MultiSelect
                options={uniqueCSMs}
                value={selectedCSMs}
                onChange={(value) => { setSelectedCSMs(value); setCurrentPage(1); }}
                placeholder="All CSMs"
              />
            </div>

            {/* Status - Third */}
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

            {/* Current month only filters */}
            {timePeriod === 'current_month' && (
              <>
                <div className="flex items-center gap-2">
                  <label className="text-sm font-medium text-gray-700">Risk Level:</label>
                  <Select value={selectedRiskLevel} onValueChange={(value) => { setSelectedRiskLevel(value); setCurrentPage(1); }}>
                    <SelectTrigger className="w-32">
                      <SelectValue placeholder="All Levels" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Levels</SelectItem>
                      {uniqueRiskLevels.map((level) => (
                        <SelectItem key={level} value={level}>{level}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex items-center gap-2">
                  <label className="text-sm font-medium text-gray-700">Trending Risk:</label>
                  <Select value={selectedTrendingRiskLevel} onValueChange={(value) => { setSelectedTrendingRiskLevel(value); setCurrentPage(1); }}>
                    <SelectTrigger className="w-36">
                      <SelectValue placeholder="All Trending" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Trending</SelectItem>
                      {uniqueTrendingRiskLevels.map((level) => (
                        <SelectItem key={level} value={level}>{level}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex items-center gap-2">
                  <label className="text-sm font-medium text-gray-700">Risk Reasons:</label>
                  <MultiSelect
                    options={uniqueRiskReasons}
                    value={selectedRiskReasons}
                    onChange={(value) => { setSelectedRiskReasons(value); setCurrentPage(1); }}
                    placeholder="All Selected"
                  />
                </div>

                <div className="flex items-center gap-2">
                  <label className="text-sm font-medium text-gray-700">Trending Reasons:</label>
                  <MultiSelect
                    options={uniqueTrendingRiskReasons}
                    value={selectedTrendingRiskReasons}
                    onChange={(value) => { setSelectedTrendingRiskReasons(value); setCurrentPage(1); }}
                    placeholder="All Selected"
                  />
                </div>
              </>
            )}

            <div className="text-sm text-gray-600 ml-auto">
              {accounts && (
                <span>Showing {paginatedAccounts.length} of {sortedAccounts.length} accounts ({accounts.length} total)</span>
              )}
            </div>
          </div>
        </div>

        {/* Summary Section - Fixed grid layout for comparison periods */}
        <div className={`mb-6 grid gap-4 w-full ${timePeriod === 'current_month' 
          ? 'grid-cols-1 md:grid-cols-2 lg:grid-cols-4 xl:grid-cols-7' 
          : 'grid-cols-1 sm:grid-cols-2 md:grid-cols-4'
        }`}>
          {/* Total Spend */}
          <div className="bg-purple-50 p-4 rounded-lg border">
            <div className="text-sm font-bold text-purple-800 mb-3">Total Spend</div>
            <div className={`${timePeriod === 'current_month' && !comparisonMetrics ? 'flex items-center min-h-[40px] w-full' : 'space-y-2'}`}>
              <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                <span className="text-xs text-gray-600">{timePeriod === 'current_month' ? 'Current Month' : 'Current Period'}</span>
                <span className="text-sm font-bold text-purple-600">{formatCurrencyWhole(summaryStats.totalSpend)}</span>
              </div>
              {comparisonMetrics && (
                <>
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">All Accounts</span>
                    <span className="text-sm font-bold text-gray-600">{formatCurrencyWhole(baselineMetrics.total_spend)}</span>
                  </div>
                  <div className="flex justify-between items-center pt-1 border-t">
                    <span className="text-xs text-gray-600">Delta</span>
                    <div className="text-sm font-bold">
                      {formatDelta(summaryStats.totalSpend - baselineMetrics.total_spend, 'currency')}
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
          
          {/* Total Texts */}
          <div className="bg-orange-50 p-4 rounded-lg border">
            <div className="text-sm font-bold text-orange-800 mb-3">Total Texts</div>
            <div className={`${timePeriod === 'current_month' && !comparisonMetrics ? 'flex items-center min-h-[40px] w-full' : 'space-y-2'}`}>
              <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                <span className="text-xs text-gray-600">{timePeriod === 'current_month' ? 'Current Month' : 'Current Period'}</span>
                <span className="text-sm font-bold text-orange-600">{summaryStats.totalTexts.toLocaleString()}</span>
              </div>
              {comparisonMetrics && (
                <>
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">All Accounts</span>
                    <span className="text-sm font-bold text-gray-600">{baselineMetrics.total_texts.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between items-center pt-1 border-t">
                    <span className="text-xs text-gray-600">Delta</span>
                    <div className="text-sm font-bold">
                      {formatDelta(summaryStats.totalTexts - baselineMetrics.total_texts, 'number')}
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
          
          {/* Total Redemptions */}
          <div className="bg-green-50 p-4 rounded-lg border">
            <div className="text-sm font-bold text-green-800 mb-3">Total Redemptions</div>
            <div className={`${timePeriod === 'current_month' && !comparisonMetrics ? 'flex items-center min-h-[40px] w-full' : 'space-y-2'}`}>
              <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                <span className="text-xs text-gray-600">{timePeriod === 'current_month' ? 'Current Month' : 'Current Period'}</span>
                <span className="text-sm font-bold text-green-600">{summaryStats.totalRedemptions.toLocaleString()}</span>
              </div>
              {comparisonMetrics && (
                <>
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">All Accounts</span>
                    <span className="text-sm font-bold text-gray-600">{baselineMetrics.total_redemptions.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between items-center pt-1 border-t">
                    <span className="text-xs text-gray-600">Delta</span>
                    <div className="text-sm font-bold">
                      {formatDelta(summaryStats.totalRedemptions - baselineMetrics.total_redemptions, 'number')}
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
          
          {/* Total Subscribers */}
          <div className="bg-blue-50 p-4 rounded-lg border">
            <div className="text-sm font-bold text-blue-800 mb-3">Total Subscribers</div>
            <div className={`${timePeriod === 'current_month' && !comparisonMetrics ? 'flex items-center min-h-[40px] w-full' : 'space-y-2'}`}>
              <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                <span className="text-xs text-gray-600">{timePeriod === 'current_month' ? 'Current Month' : 'Current Period'}</span>
                <span className="text-sm font-bold text-blue-600">{summaryStats.totalSubscribers.toLocaleString()}</span>
              </div>
              {comparisonMetrics && (
                <>
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">All Accounts</span>
                    <span className="text-sm font-bold text-gray-600">{baselineMetrics.total_subscribers.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between items-center pt-1 border-t">
                    <span className="text-xs text-gray-600">Delta</span>
                    <div className="text-sm font-bold">
                      {formatDelta(summaryStats.totalSubscribers - baselineMetrics.total_subscribers, 'number')}
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
          
          {/* Risk Level Summary Cards - Only show for current month */}
          {timePeriod === 'current_month' && (
            <>
              {/* High Risk Count */}
              <div className="bg-red-50 p-4 rounded-lg border">
                <div className="text-sm font-bold text-red-800 mb-3">High Risk</div>
                <div className="space-y-2">
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">Current Month</span>
                    <span className="text-sm font-bold text-red-600">{summaryStats.highRiskCount.toLocaleString()}</span>
                  </div>
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">Trending</span>
                    <span className="text-sm font-bold text-red-600">{summaryStats.trendingHighRiskCount.toLocaleString()}</span>
                  </div>
                </div>
              </div>
              
              {/* Medium Risk Count */}
              <div className="bg-orange-50 p-4 rounded-lg border">
                <div className="text-sm font-bold text-orange-800 mb-3">Medium Risk</div>
                <div className="space-y-2">
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">Current Month</span>
                    <span className="text-sm font-bold text-orange-600">{summaryStats.medRiskCount.toLocaleString()}</span>
                  </div>
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">Trending</span>
                    <span className="text-sm font-bold text-orange-600">{summaryStats.trendingMedRiskCount.toLocaleString()}</span>
                  </div>
                </div>
              </div>
              
              {/* Low Risk Count */}
              <div className="bg-green-50 p-4 rounded-lg border">
                <div className="text-sm font-bold text-green-800 mb-3">Low Risk</div>
                <div className="space-y-2">
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">Current Month</span>
                    <span className="text-sm font-bold text-green-600">{summaryStats.lowRiskCount.toLocaleString()}</span>
                  </div>
                  <div className={`flex justify-between items-center ${timePeriod === 'current_month' && !comparisonMetrics ? 'w-full' : ''}`}>
                    <span className="text-xs text-gray-600">Trending</span>
                    <span className="text-sm font-bold text-green-600">{summaryStats.trendingLowRiskCount.toLocaleString()}</span>
                  </div>
                </div>
              </div>
            </>
          )}
        </div>

        <div className="overflow-x-auto">
          <div className="max-h-[600px] overflow-y-auto border rounded-lg">
            <table className="w-full border-collapse">
              <thead className="sticky top-0 bg-gray-50 z-10">
                <tr className="border-b">
                  <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-left" onClick={() => handleSort('name')}>
                    <div className="flex items-center gap-1">
                      Account
                      <div className="flex flex-col">
                        <ChevronUp 
                          size={12} 
                          className={`${sortField === 'name' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                        />
                        <ChevronDown 
                          size={12} 
                          className={`${sortField === 'name' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                        />
                      </div>
                    </div>
                  </th>
                  <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-left" onClick={() => handleSort('csm')}>
                    <div className="flex items-center gap-1">
                      CSM
                      <div className="flex flex-col">
                        <ChevronUp 
                          size={12} 
                          className={`${sortField === 'csm' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                        />
                        <ChevronDown 
                          size={12} 
                          className={`${sortField === 'csm' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                        />
                      </div>
                    </div>
                  </th>
                  <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-left" onClick={() => handleSort('status')}>
                    <div className="flex items-center gap-1">
                      Status
                      <div className="flex flex-col">
                        <ChevronUp 
                          size={12} 
                          className={`${sortField === 'status' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                        />
                        <ChevronDown 
                          size={12} 
                          className={`${sortField === 'status' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                        />
                      </div>
                    </div>
                  </th>
                  {comparisonMetrics && (
                    <th className="p-3 font-medium text-left">Change Label</th>
                  )}
                  <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-right" onClick={() => handleSort('total_spend')}>
                    <div className="flex items-center gap-1">
                      Total Spend
                      <div className="flex flex-col">
                        <ChevronUp 
                          size={12} 
                          className={`${sortField === 'total_spend' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                        />
                        <ChevronDown 
                          size={12} 
                          className={`${sortField === 'total_spend' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                        />
                      </div>
                    </div>
                  </th>
                  {comparisonMetrics && (
                    <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-right" onClick={() => handleSort('spend_delta')}>
                      <div className="flex items-center gap-1">
                        Spend Δ
                        <div className="flex flex-col">
                          <ChevronUp 
                            size={12} 
                            className={`${sortField === 'spend_delta' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                          />
                          <ChevronDown 
                            size={12} 
                            className={`${sortField === 'spend_delta' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                          />
                        </div>
                      </div>
                    </th>
                  )}
                  <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-right" onClick={() => handleSort('total_texts_delivered')}>
                    <div className="flex items-center gap-1">
                      Texts Delivered
                      <div className="flex flex-col">
                        <ChevronUp 
                          size={12} 
                          className={`${sortField === 'total_texts_delivered' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                        />
                        <ChevronDown 
                          size={12} 
                          className={`${sortField === 'total_texts_delivered' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                        />
                      </div>
                    </div>
                  </th>
                  {comparisonMetrics && (
                    <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-right" onClick={() => handleSort('texts_delta')}>
                      <div className="flex items-center gap-1">
                        Texts Δ
                        <div className="flex flex-col">
                          <ChevronUp 
                            size={12} 
                            className={`${sortField === 'texts_delta' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                          />
                          <ChevronDown 
                            size={12} 
                            className={`${sortField === 'texts_delta' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                          />
                        </div>
                      </div>
                    </th>
                  )}
                  <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-right" onClick={() => handleSort('coupons_redeemed')}>
                    <div className="flex items-center gap-1">
                      Redemptions
                      <div className="flex flex-col">
                        <ChevronUp 
                          size={12} 
                          className={`${sortField === 'coupons_redeemed' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                        />
                        <ChevronDown 
                          size={12} 
                          className={`${sortField === 'coupons_redeemed' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                        />
                      </div>
                    </div>
                  </th>
                  {comparisonMetrics && (
                    <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-right" onClick={() => handleSort('coupons_delta')}>
                      <div className="flex items-center gap-1">
                        Redemptions Δ
                        <div className="flex flex-col">
                          <ChevronUp 
                            size={12} 
                            className={`${sortField === 'coupons_delta' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                          />
                          <ChevronDown 
                            size={12} 
                            className={`${sortField === 'coupons_delta' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                          />
                        </div>
                      </div>
                    </th>
                  )}
                  <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-right" onClick={() => handleSort('active_subs_cnt')}>
                    <div className="flex items-center gap-1">
                      Subscribers
                      <div className="flex flex-col">
                        <ChevronUp 
                          size={12} 
                          className={`${sortField === 'active_subs_cnt' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                        />
                        <ChevronDown 
                          size={12} 
                          className={`${sortField === 'active_subs_cnt' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                        />
                      </div>
                    </div>
                  </th>
                  {comparisonMetrics && (
                    <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-right" onClick={() => handleSort('subs_delta')}>
                      <div className="flex items-center gap-1">
                        Subscribers Δ
                        <div className="flex flex-col">
                          <ChevronUp 
                            size={12} 
                            className={`${sortField === 'subs_delta' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                          />
                          <ChevronDown 
                            size={12} 
                            className={`${sortField === 'subs_delta' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                          />
                        </div>
                      </div>
                    </th>
                  )}
                  {timePeriod === 'current_month' && (
                    <>
                      <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-left" onClick={() => handleSort('risk_level')}>
                        <div className="flex items-center gap-1">
                          Risk Level
                          <div className="flex flex-col">
                            <ChevronUp 
                              size={12} 
                              className={`${sortField === 'risk_level' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                            />
                            <ChevronDown 
                              size={12} 
                              className={`${sortField === 'risk_level' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                            />
                          </div>
                        </div>
                      </th>
                      <th className="p-3 font-medium text-left">
                        Risk Level Reason
                      </th>
                      <th className="p-3 font-medium cursor-pointer hover:bg-gray-100 transition-colors text-left" onClick={() => handleSort('trending_risk_level')}>
                        <div className="flex items-center gap-1">
                          Trending Risk Level
                          <div className="flex flex-col">
                            <ChevronUp 
                              size={12} 
                              className={`${sortField === 'trending_risk_level' && sortDirection === 'asc' ? 'text-blue-600' : 'text-gray-400'}`} 
                            />
                            <ChevronDown 
                              size={12} 
                              className={`${sortField === 'trending_risk_level' && sortDirection === 'desc' ? 'text-blue-600' : 'text-gray-400'} -mt-1`} 
                            />
                          </div>
                        </div>
                      </th>
                      <th className="p-3 font-medium text-left">
                        Trending Risk Reason
                      </th>
                    </>
                  )}
                </tr>
              </thead>
              <tbody>
                {paginatedAccounts.map((account, index) => (
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
                    {comparisonMetrics && (
                      <td className="p-3">
                        {account.status_label && (
                          <Badge variant="outline" className="text-xs">
                            {account.status_label}
                          </Badge>
                        )}
                      </td>
                    )}
                    <td className="p-3 text-right font-mono text-sm">
                      {formatCurrency(account.total_spend)}
                    </td>
                    {comparisonMetrics && (
                      <td className="p-3 text-right font-mono text-sm">
                        {formatDelta(account.spend_delta || 0, 'currency')}
                      </td>
                    )}
                    <td className="p-3 text-right font-mono text-sm">
                      {formatNumber(timePeriod === 'current_month' 
                        ? account.total_texts_delivered 
                        : (account.total_texts_delivered || 0)
                      )}
                    </td>
                    {comparisonMetrics && (
                      <td className="p-3 text-right font-mono text-sm">
                        {formatDelta(account.texts_delta || 0, 'number')}
                      </td>
                    )}
                    <td className="p-3 text-right font-mono text-sm">
                      {formatNumber(timePeriod === 'current_month' 
                        ? account.coupons_redeemed 
                        : (account.coupons_redeemed || 0)
                      )}
                    </td>
                    {comparisonMetrics && (
                      <td className="p-3 text-right font-mono text-sm">
                        {formatDelta(account.coupons_delta || 0, 'number')}
                      </td>
                    )}
                    <td className="p-3 text-right font-mono text-sm">
                      {formatNumber(timePeriod === 'current_month' 
                        ? account.active_subs_cnt 
                        : (account.active_subs_cnt || 0)
                      )}
                    </td>
                    {comparisonMetrics && (
                      <td className="p-3 text-right font-mono text-sm">
                        {formatDelta(account.subs_delta || 0, 'number')}
                      </td>
                    )}
                    {timePeriod === 'current_month' && (
                      <>
                        <td className="p-3">
                          {(account.riskLevel || account.risk_level) && (
                            <Badge variant={getRiskBadge(account.riskLevel || account.risk_level)} className="text-xs">
                              {(account.riskLevel || account.risk_level).toUpperCase()}
                            </Badge>
                          )}
                        </td>
                        <td className="p-3 text-xs text-gray-600 max-w-48">
                          {getRiskReasons(account, false).join(', ')}
                        </td>
                        <td className="p-3">
                          {account.trending_risk_level && (
                            <Badge variant={getRiskBadge(account.trending_risk_level)} className="text-xs">
                              {account.trending_risk_level.toUpperCase()}
                            </Badge>
                          )}
                        </td>
                        <td className="p-3 text-xs text-gray-600 max-w-48">
                          {getRiskReasons(account, true).join(', ')}
                        </td>
                      </>
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

    {/* Account Detail Modal */}
    {selectedAccountId && (
      <AccountDetailModal
        accountId={selectedAccountId}
        accountName={selectedAccountName}
        isOpen={isModalOpen}
        onClose={handleModalClose}
      />
    )}
    </>
  );
}