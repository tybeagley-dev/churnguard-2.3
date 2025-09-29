import { useQuery } from "@tanstack/react-query";

export function useMonthlyAccounts(comparison?: string, filters?: {status?: string, csm_owner?: string, risk_level?: string}) {
  return useQuery({
    queryKey: ['/api/account-metrics-monthly', comparison, filters],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (comparison) {
        params.append('comparison', comparison);
      }
      if (filters?.status && filters.status !== 'all') {
        params.append('status', filters.status);
      }
      if (filters?.csm_owner && filters.csm_owner !== 'all') {
        params.append('csm_owner', filters.csm_owner);
      }
      if (filters?.risk_level && filters.risk_level !== 'all') {
        params.append('risk_level', filters.risk_level);
      }
      const url = `/api/account-metrics-monthly${params.toString() ? '?' + params.toString() : ''}`;
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error('Failed to fetch monthly accounts');
      }
      return response.json();
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

export function useMonthlyAccountHistory(accountId: string) {
  return useQuery({
    queryKey: ['/api/account-history-monthly', accountId],
    queryFn: async () => {
      const response = await fetch(`/api/account-history-monthly/${accountId}`, {
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          'Pragma': 'no-cache',
          'Expires': '0'
        }
      });
      if (!response.ok) {
        throw new Error('Failed to fetch monthly account history');
      }
      return response.json();
    },
    enabled: !!accountId,
    staleTime: 10 * 60 * 1000, // 10 minutes
  });
}