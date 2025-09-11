import { useQuery } from "@tanstack/react-query";

export function useMonthlyAccounts(period: string = 'current_month') {
  return useQuery({
    queryKey: ['/api/bigquery/accounts/monthly', period],
    queryFn: async () => {
      const response = await fetch(`/api/bigquery/accounts/monthly?period=${period}`);
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