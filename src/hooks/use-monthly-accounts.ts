import { useQuery } from "@tanstack/react-query";

export function useMonthlyAccounts(comparison?: string) {
  return useQuery({
    queryKey: ['/api/account-metrics-monthly', comparison],
    queryFn: async () => {
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
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

export function useMonthlyAccountHistory(accountId: string) {
  return useQuery({
    queryKey: ['/api/bigquery/account-history/monthly', accountId],
    queryFn: async () => {
      const response = await fetch(`/api/bigquery/account-history/monthly/${accountId}`);
      if (!response.ok) {
        throw new Error('Failed to fetch monthly account history');
      }
      return response.json();
    },
    enabled: !!accountId,
    staleTime: 10 * 60 * 1000, // 10 minutes
  });
}