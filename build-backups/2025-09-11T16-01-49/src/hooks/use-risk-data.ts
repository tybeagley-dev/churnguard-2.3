import { useQuery } from "@tanstack/react-query";
import { RiskScore } from "@shared/schema";
import { DashboardAnalytics } from "@/types/api";

export function useRiskScores() {
  return useQuery<RiskScore[]>({
    queryKey: ["/api/risk-scores/latest"],
  });
}

export function useAccountRiskHistory(accountId: number) {
  return useQuery<RiskScore[]>({
    queryKey: [`/api/accounts/${accountId}/risk-scores`],
    enabled: !!accountId,
  });
}

export function useDashboardAnalytics() {
  return useQuery<DashboardAnalytics>({
    queryKey: ["/api/analytics/dashboard"],
  });
}
