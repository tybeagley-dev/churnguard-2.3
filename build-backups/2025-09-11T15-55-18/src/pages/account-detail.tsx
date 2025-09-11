import { useParams } from "wouter";
import { useAccount, useAccountRiskHistory } from "@/hooks/use-accounts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ArrowLeft, Mail, Phone, FileText } from "lucide-react";
import { Link } from "wouter";

export default function AccountDetail() {
  const { id } = useParams();
  const accountId = parseInt(id || "0");
  
  const { data: account, isLoading } = useAccount(accountId);
  const { data: riskHistory } = useAccountRiskHistory(accountId);

  if (isLoading) {
    return <div className="flex items-center justify-center h-screen">Loading...</div>;
  }

  if (!account) {
    return <div className="flex items-center justify-center h-screen">Account not found</div>;
  }

  const getRiskLevelColor = (level: string) => {
    switch (level) {
      case "high": return "bg-red-100 text-red-800";
      case "medium": return "bg-yellow-100 text-yellow-800";
      case "low": return "bg-green-100 text-green-800";
      default: return "bg-gray-100 text-gray-800";
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto py-6 px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <Link href="/">
            <Button variant="ghost" size="sm">
              <ArrowLeft className="h-4 w-4 mr-2" />
              Back to Dashboard
            </Button>
          </Link>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Account Information */}
          <div className="lg:col-span-2 space-y-6">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-2xl">{account.name}</CardTitle>
                  <Badge className={getRiskLevelColor(account.riskLevel)}>
                    {account.riskLevel.toUpperCase()} RISK
                  </Badge>
                </div>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-sm text-gray-600">Location</p>
                    <p className="font-medium">{account.location}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-600">Monthly Revenue</p>
                    <p className="font-medium">${parseFloat(account.monthlyRevenue).toLocaleString()}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-600">CSM</p>
                    <p className="font-medium">{account.csmName}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-600">Risk Score</p>
                    <p className="font-medium">{account.currentRiskScore}/100</p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Tabs defaultValue="overview" className="w-full">
              <TabsList>
                <TabsTrigger value="overview">Overview</TabsTrigger>
                <TabsTrigger value="history">History</TabsTrigger>
                <TabsTrigger value="communications">Communications</TabsTrigger>
                <TabsTrigger value="notes">Notes</TabsTrigger>
              </TabsList>
              
              <TabsContent value="overview" className="space-y-4">
                <Card>
                  <CardHeader>
                    <CardTitle>Risk Analysis</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      <div className="h-64 bg-gray-100 rounded-lg flex items-center justify-center">
                        <p className="text-gray-500">Risk timeline chart would go here</p>
                      </div>
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <p className="text-sm text-gray-600">Primary Risk Factors</p>
                          <div className="flex flex-wrap gap-2 mt-2">
                            <Badge variant="destructive">Engagement Drop</Badge>
                            <Badge variant="secondary">Support Issues</Badge>
                          </div>
                        </div>
                        <div>
                          <p className="text-sm text-gray-600">Last Updated</p>
                          <p className="font-medium">2 minutes ago</p>
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>
              
              <TabsContent value="history">
                <Card>
                  <CardHeader>
                    <CardTitle>Account History</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-gray-500">Historical data and changes would be displayed here</p>
                  </CardContent>
                </Card>
              </TabsContent>
              
              <TabsContent value="communications">
                <Card>
                  <CardHeader>
                    <CardTitle>Communication History</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-gray-500">Email and call history would be displayed here</p>
                  </CardContent>
                </Card>
              </TabsContent>
              
              <TabsContent value="notes">
                <Card>
                  <CardHeader>
                    <CardTitle>Notes & Actions</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-gray-500">Notes and action items would be displayed here</p>
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>
          </div>

          {/* Action Panel */}
          <div className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle>Quick Actions</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <Button className="w-full justify-start">
                  <Mail className="h-4 w-4 mr-2" />
                  Send Email
                </Button>
                <Button className="w-full justify-start" variant="outline">
                  <Phone className="h-4 w-4 mr-2" />
                  Schedule Call
                </Button>
                <Button className="w-full justify-start" variant="outline">
                  <FileText className="h-4 w-4 mr-2" />
                  Add Note
                </Button>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Risk Score Timeline</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="h-48 bg-gray-100 rounded-lg flex items-center justify-center">
                  <p className="text-gray-500">Risk score timeline</p>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}
