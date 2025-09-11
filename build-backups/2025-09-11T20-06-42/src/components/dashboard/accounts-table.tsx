import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Mail, Phone, FileText, Download, Plus, Utensils } from "lucide-react";
import { useFilteredAccounts } from "@/hooks/use-accounts";
import { FilterOptions } from "@/types/api";

interface AccountsTableProps {
  filters: FilterOptions;
  onAccountSelect: (accountId: number) => void;
}

export default function AccountsTable({ filters, onAccountSelect }: AccountsTableProps) {
  const { data: accounts, isLoading } = useFilteredAccounts(filters);

  const getRiskBadgeColor = (riskLevel: string) => {
    switch (riskLevel) {
      case "high": return "bg-red-100 text-red-800";
      case "medium": return "bg-yellow-100 text-yellow-800";
      case "low": return "bg-green-100 text-green-800";
      default: return "bg-gray-100 text-gray-800";
    }
  };

  const getRiskScoreColor = (score: number) => {
    if (score >= 80) return "bg-red-100 text-red-600";
    if (score >= 50) return "bg-yellow-100 text-yellow-600";
    return "bg-green-100 text-green-600";
  };

  const getRiskFlags = (account: any) => {
    const flags = [];
    if (account.currentRiskScore >= 80) {
      flags.push({ label: "Engagement Drop", color: "bg-yellow-100 text-yellow-800" });
      flags.push({ label: "Support Issues", color: "bg-red-100 text-red-800" });
    }
    return flags;
  };

  if (isLoading) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="animate-pulse">
            <div className="h-8 bg-gray-200 rounded mb-4"></div>
            <div className="space-y-3">
              {[...Array(5)].map((_, i) => (
                <div key={i} className="h-16 bg-gray-200 rounded"></div>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  const highRiskAccounts = accounts?.filter(account => account.riskLevel === "high") || [];

  return (
    <Card className="shadow-sm border border-gray-200">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg font-semibold text-gray-900">
            High-Risk Accounts
          </CardTitle>
          <div className="flex space-x-2">
            <Button variant="outline" size="sm">
              <Download className="h-4 w-4 mr-1" />
              Export
            </Button>
            <Button size="sm" className="bg-primary hover:bg-primary/90">
              <Plus className="h-4 w-4 mr-1" />
              Add Note
            </Button>
          </div>
        </div>
      </CardHeader>
      
      <CardContent>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Account</TableHead>
                <TableHead>Risk Score</TableHead>
                <TableHead>Primary Flags</TableHead>
                <TableHead>Revenue</TableHead>
                <TableHead>CSM</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {highRiskAccounts.map((account) => {
                const flags = getRiskFlags(account);
                
                return (
                  <TableRow 
                    key={account.id} 
                    className="hover:bg-gray-50 cursor-pointer"
                    onClick={() => onAccountSelect(account.id)}
                  >
                    <TableCell>
                      <div className="flex items-center">
                        <div className="w-10 h-10 bg-gray-200 rounded-lg flex items-center justify-center mr-3">
                          <Utensils className="h-5 w-5 text-gray-600" />
                        </div>
                        <div>
                          <div className="text-sm font-medium text-gray-900">{account.name}</div>
                          <div className="text-sm text-gray-500">{account.location}</div>
                        </div>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <div className={`w-12 h-12 rounded-lg flex items-center justify-center mr-3 ${getRiskScoreColor(account.currentRiskScore)}`}>
                          <span className="text-sm font-bold">{account.currentRiskScore}</span>
                        </div>
                        <Badge className={getRiskBadgeColor(account.riskLevel)}>
                          {account.riskLevel.toUpperCase()} RISK
                        </Badge>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {flags.map((flag, index) => (
                          <Badge key={index} variant="outline" className={flag.color}>
                            {flag.label}
                          </Badge>
                        ))}
                      </div>
                    </TableCell>
                    <TableCell className="text-sm text-gray-900">
                      ${parseFloat(account.monthlyRevenue).toLocaleString()}/mo
                    </TableCell>
                    <TableCell className="text-sm text-gray-900">
                      {account.csmName}
                    </TableCell>
                    <TableCell>
                      <div className="flex space-x-2">
                        <Button variant="ghost" size="sm" className="text-indigo-600 hover:text-indigo-900">
                          <Mail className="h-4 w-4" />
                        </Button>
                        <Button variant="ghost" size="sm" className="text-green-600 hover:text-green-900">
                          <Phone className="h-4 w-4" />
                        </Button>
                        <Button variant="ghost" size="sm" className="text-blue-600 hover:text-blue-900">
                          <FileText className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
        
        <div className="flex items-center justify-between mt-4 pt-4 border-t border-gray-200">
          <p className="text-sm text-gray-500">
            Showing 1 to {highRiskAccounts.length} of {highRiskAccounts.length} high-risk accounts
          </p>
          <div className="flex space-x-2">
            <Button variant="outline" size="sm">
              Previous
            </Button>
            <Button size="sm" className="bg-primary hover:bg-primary/90">
              Next
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
