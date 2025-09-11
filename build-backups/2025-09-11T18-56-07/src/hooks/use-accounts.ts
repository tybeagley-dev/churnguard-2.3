import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import { Account, Action, InsertAction, RiskScore } from "@shared/schema";
import { AccountWithRiskData, FilterOptions } from "@/types/api";
import React from "react";

export function useAccounts() {
  return useQuery<Account[]>({
    queryKey: ["/api/accounts"],
  });
}

export function useAccount(id: number) {
  return useQuery<Account>({
    queryKey: [`/api/accounts/${id}`],
    enabled: !!id,
  });
}

export function useAccountsByRiskLevel(level: string) {
  return useQuery<Account[]>({
    queryKey: [`/api/accounts/risk/${level}`],
    enabled: !!level && level !== "all",
  });
}

export function useAccountActions(accountId: number) {
  return useQuery<Action[]>({
    queryKey: [`/api/accounts/${accountId}/actions`],
    enabled: !!accountId,
  });
}

export function useCreateAction() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async ({ accountId, ...data }: InsertAction & { accountId: number }) => {
      const response = await apiRequest("POST", `/api/accounts/${accountId}/actions`, data);
      return response.json();
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({
        queryKey: [`/api/accounts/${variables.accountId}/actions`],
      });
    },
  });
}

export function useUpdateAccountRisk() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (accountId: number) => {
      const response = await apiRequest("POST", `/api/accounts/${accountId}/update-risk`, {});
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/accounts"] });
      queryClient.invalidateQueries({ queryKey: ["/api/risk-scores/latest"] });
      queryClient.invalidateQueries({ queryKey: ["/api/analytics/dashboard"] });
    },
  });
}

export function useUpdateAllRiskScores() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async () => {
      const response = await apiRequest("POST", "/api/risk-scores/update-all", {});
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/accounts"] });
      queryClient.invalidateQueries({ queryKey: ["/api/risk-scores/latest"] });
      queryClient.invalidateQueries({ queryKey: ["/api/analytics/dashboard"] });
    },
  });
}

export function useSearchAccounts(query: string) {
  return useQuery<Account[]>({
    queryKey: [`/api/accounts/search`, query],
    enabled: !!query && query.length > 0,
  });
}

export function useAccountRiskHistory(accountId: number) {
  return useQuery<RiskScore[]>({
    queryKey: [`/api/accounts/${accountId}/risk-scores`],
    enabled: !!accountId,
  });
}

export function useFilteredAccounts(filters: FilterOptions) {
  const { data: allAccounts, isLoading } = useAccounts();
  const { data: searchResults, isLoading: isSearching } = useSearchAccounts(filters.searchQuery || "");
  
  const filteredAccounts = React.useMemo(() => {
    if (!allAccounts) return [];
    
    let accounts = allAccounts;
    
    // Apply search filter
    if (filters.searchQuery && searchResults) {
      accounts = searchResults;
    }
    
    // Apply risk level filter
    if (filters.riskLevel && filters.riskLevel !== "all") {
      accounts = accounts.filter(account => account.riskLevel === filters.riskLevel);
    }
    
    return accounts;
  }, [allAccounts, searchResults, filters]);
  
  return {
    data: filteredAccounts,
    isLoading: isLoading || isSearching,
  };
}
