export interface DashboardAnalytics {
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
}

export interface RiskFlag {
  type: 'engagement' | 'support' | 'billing' | 'usage';
  label: string;
  color: string;
}

export interface AccountWithRiskData {
  id: number;
  name: string;
  location: string;
  monthlyRevenue: string;
  csmName: string;
  currentRiskScore: number;
  riskLevel: 'low' | 'medium' | 'high';
  flags: RiskFlag[];
  metadata?: {
    engagementDrop?: number;
    supportTicketCount?: number;
    billingIssueDetails?: string;
    usageDropPercentage?: number;
  };
}

export interface FilterOptions {
  riskLevel?: 'all' | 'high' | 'medium' | 'low';
  searchQuery?: string;
}
