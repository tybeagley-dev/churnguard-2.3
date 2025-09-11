import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Search, RefreshCw, Download } from "lucide-react";
import { useUpdateAllRiskScores } from "@/hooks/use-accounts";
import { useToast } from "@/hooks/use-toast";
import { FilterOptions } from "@/types/api";
import { apiRequest } from "@/lib/queryClient";

interface HeaderProps {
  filters: FilterOptions;
  onFilterChange: (filters: Partial<FilterOptions>) => void;
}

export default function Header({ filters, onFilterChange }: HeaderProps) {
  const { toast } = useToast();
  const updateAllRiskScores = useUpdateAllRiskScores();

  const handleRefresh = () => {
    updateAllRiskScores.mutate(undefined, {
      onSuccess: () => {
        toast({
          title: "Success",
          description: "All risk scores updated successfully",
        });
      },
      onError: () => {
        toast({
          title: "Error",
          description: "Failed to update risk scores",
          variant: "destructive",
        });
      },
    });
  };



  const syncBigQueryData = async () => {
    try {
      const response = await apiRequest("POST", "/api/sync/bigquery", {});
      const result = await response.json();
      
      if (result.status === "success") {
        toast({
          title: "Data Sync Successful",
          description: `Synchronized ${result.accountsSynced} accounts from BigQuery`,
        });
        // Refresh the page data
        window.location.reload();
      } else {
        throw new Error(result.message);
      }
    } catch (error) {
      toast({
        title: "Data Sync Failed",
        description: error instanceof Error ? error.message : "Unknown error",
        variant: "destructive",
      });
    }
  };

  return (
    <header className="bg-white shadow-sm border-b border-gray-200 px-6 py-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <h2 className="text-2xl font-semibold text-gray-900">Dashboard</h2>
          <div className="text-sm text-gray-500">
            Last updated: <span>2 minutes ago</span>
          </div>
        </div>
        
        <div className="flex items-center space-x-4">
          <div className="relative">
            <Input
              type="text"
              placeholder="Search accounts..."
              value={filters.searchQuery || ""}
              onChange={(e) => onFilterChange({ searchQuery: e.target.value })}
              className="w-64 pl-10"
            />
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
          </div>
          
          <Select
            value={filters.riskLevel || "all"}
            onValueChange={(value) => onFilterChange({ riskLevel: value as FilterOptions["riskLevel"] })}
          >
            <SelectTrigger className="w-40">
              <SelectValue placeholder="Risk Level" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Risk Levels</SelectItem>
              <SelectItem value="high">High Risk</SelectItem>
              <SelectItem value="medium">Medium Risk</SelectItem>
              <SelectItem value="low">Low Risk</SelectItem>
            </SelectContent>
          </Select>
          
          <Button 
            onClick={syncBigQueryData}
            variant="outline"
            className="mr-2"
          >
            <Download className="h-4 w-4 mr-2" />
            Sync Data
          </Button>
          
          <Button 
            onClick={handleRefresh} 
            disabled={updateAllRiskScores.isPending}
            className="bg-primary hover:bg-primary/90"
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${updateAllRiskScores.isPending ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>
      </div>
    </header>
  );
}
