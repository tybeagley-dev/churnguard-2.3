import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Mail, Phone, FileText, X } from "lucide-react";
import { useAccount, useAccountRiskHistory } from "@/hooks/use-accounts";
import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer } from "recharts";

interface AccountDetailModalProps {
  accountId: number;
  onClose: () => void;
}

export default function AccountDetailModal({ accountId, onClose }: AccountDetailModalProps) {
  const { data: account, isLoading } = useAccount(accountId);
  const { data: riskHistory } = useAccountRiskHistory(accountId);

  if (isLoading) {
    return (
      <Dialog open={true} onOpenChange={onClose}>
        <DialogContent className="max-w-4xl">
          <div className="animate-pulse">
            <div className="h-8 bg-gray-200 rounded mb-4"></div>
            <div className="h-64 bg-gray-200 rounded"></div>
          </div>
        </DialogContent>
      </Dialog>
    );
  }

  if (!account) {
    return null;
  }

  const getRiskLevelColor = (level: string) => {
    switch (level) {
      case "high": return "bg-red-100 text-red-800";
      case "medium": return "bg-yellow-100 text-yellow-800";
      case "low": return "bg-green-100 text-green-800";
      default: return "bg-gray-100 text-gray-800";
    }
  };

  // Mock risk history data for visualization
  const mockRiskHistory = [
    { month: "Jan", score: 35 },
    { month: "Feb", score: 42 },
    { month: "Mar", score: 58 },
    { month: "Apr", score: 72 },
    { month: "May", score: 85 },
    { month: "Jun", score: account?.currentRiskScore || 65 },
  ];

  return (
    <Dialog open={true} onOpenChange={onClose}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center justify-between">
            <span>Account Details</span>
            <Button variant="ghost" size="sm" onClick={onClose}>
              <X className="h-4 w-4" />
            </Button>
          </DialogTitle>
        </DialogHeader>
        
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Account Information */}
          <div className="space-y-6">
            <Card className="bg-gray-50">
              <CardHeader>
                <CardTitle className="text-base">Account Information</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600">Restaurant:</span>
                  <span className="text-sm font-medium text-gray-900">{account.name}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600">Location:</span>
                  <span className="text-sm font-medium text-gray-900">{account.location}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600">Monthly Revenue:</span>
                  <span className="text-sm font-medium text-gray-900">
                    ${parseFloat(account.monthlyRevenue).toLocaleString()}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600">CSM:</span>
                  <span className="text-sm font-medium text-gray-900">{account.csmName}</span>
                </div>
              </CardContent>
            </Card>
            
            {/* Risk Flags */}
            <Card className="bg-red-50">
              <CardHeader>
                <CardTitle className="text-base text-red-900">Active Risk Flags</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <div className="w-3 h-3 bg-red-500 rounded-full mr-3"></div>
                    <span className="text-sm font-medium text-red-900">Engagement Drop</span>
                  </div>
                  <span className="text-xs text-red-700">-45% last 30 days</span>
                </div>
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <div className="w-3 h-3 bg-yellow-500 rounded-full mr-3"></div>
                    <span className="text-sm font-medium text-yellow-900">Support Issues</span>
                  </div>
                  <span className="text-xs text-yellow-700">8 tickets this month</span>
                </div>
              </CardContent>
            </Card>
          </div>
          
          {/* Action Panel */}
          <div className="space-y-6">
            <Card className="bg-blue-50">
              <CardHeader>
                <CardTitle className="text-base text-blue-900">Quick Actions</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <Button className="w-full bg-blue-600 hover:bg-blue-700">
                  <Mail className="h-4 w-4 mr-2" />
                  Send Email
                </Button>
                <Button className="w-full bg-green-600 hover:bg-green-700">
                  <Phone className="h-4 w-4 mr-2" />
                  Schedule Call
                </Button>
                <Button className="w-full bg-purple-600 hover:bg-purple-700">
                  <FileText className="h-4 w-4 mr-2" />
                  Add Note
                </Button>
              </CardContent>
            </Card>
            
            {/* Risk Score Timeline */}
            <Card className="bg-gray-50">
              <CardHeader>
                <CardTitle className="text-base">Risk Score History</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="h-32">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={mockRiskHistory}>
                      <XAxis dataKey="month" />
                      <YAxis />
                      <Bar dataKey="score" fill="#EF4444" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
