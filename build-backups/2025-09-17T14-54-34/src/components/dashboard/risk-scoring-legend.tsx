import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { AlertTriangle, TrendingDown, Users, DollarSign, Target, ChevronDown, ChevronUp, RefreshCw, Loader2 } from "lucide-react";

export default function RiskScoringLegend() {
  const [isOpen, setIsOpen] = useState(false);
  const [isHubSpotOpen, setIsHubSpotOpen] = useState(false);
  const [hubSpotLoading, setHubSpotLoading] = useState(false);

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const handleHubSpotSync = async () => {
    setHubSpotLoading(true);
    try {
      const response = await fetch('/api/hubspot/bulk-sync', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('auth_token')}`
        },
        body: JSON.stringify({})
      });
      
      if (response.ok) {
        const result = await response.json();
        console.log('HubSpot sync completed:', result);
      }
    } catch (error) {
      console.error('HubSpot sync failed:', error);
    } finally {
      setHubSpotLoading(false);
    }
  };

  return (
    <div className="mt-8 space-y-6">
      {/* Main Title with Toggle */}
      <div className="text-center">
        <div className="flex items-center justify-center gap-4 mb-2">
          <h2 className="text-2xl font-bold text-gray-900">Risk Scoring System</h2>
          <Button
            variant="outline"
            size="sm"
            onClick={handleToggle}
            className="flex items-center gap-2"
          >
            {isOpen ? (
              <>
                Hide Details <ChevronUp className="h-4 w-4" />
              </>
            ) : (
              <>
                Show Details <ChevronDown className="h-4 w-4" />
              </>
            )}
          </Button>
        </div>
        <p className="text-gray-600">Understanding how we identify accounts at risk of churning</p>
      </div>

      {/* Collapsible Content */}
      <div className={`space-y-6 transition-all duration-300 ${isOpen ? 'block opacity-100' : 'hidden opacity-0'}`}>

      {/* Risk Level Overview */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Target className="h-5 w-5" />
            Risk Level Classification
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="text-center p-4 bg-red-50 rounded-lg border border-red-200">
              <Badge variant="destructive" className="mb-2">HIGH RISK</Badge>
              <p className="text-sm text-gray-700">3-4 risk flags active OR Account Frozen and 1+ months since last text</p>
              <p className="text-xs text-gray-500 mt-1">Immediate attention required</p>
            </div>
            <div className="text-center p-4 bg-gray-50 rounded-lg border border-gray-200">
              <Badge variant="secondary" className="mb-2">MEDIUM RISK</Badge>
              <p className="text-sm text-gray-700">1-2 risk flags active OR Account Frozen</p>
              <p className="text-xs text-gray-500 mt-1">Monitor closely</p>
            </div>
            <div className="text-center p-4 bg-green-50 rounded-lg border border-green-200">
              <Badge variant="outline" className="mb-2 text-green-700 border-green-700">LOW RISK</Badge>
              <p className="text-sm text-gray-700">0 risk flags active</p>
              <p className="text-xs text-gray-500 mt-1">Healthy account</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Individual Risk Flags */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5" />
            Risk Flags Explained
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div className="border-l-4 border-red-500 pl-4">
                <div className="flex items-center gap-2 mb-1">
                  <TrendingDown className="h-4 w-4 text-red-500" />
                  <h4 className="font-semibold text-red-700">Monthly Redemptions</h4>
                </div>
                <p className="text-sm text-gray-600">Less than 10 redemptions in the current month</p>
              </div>
              
              <div className="border-l-4 border-orange-500 pl-4">
                <div className="flex items-center gap-2 mb-1">
                  <Users className="h-4 w-4 text-orange-500" />
                  <h4 className="font-semibold text-orange-700">Low Activity</h4>
                </div>
                <p className="text-sm text-gray-600">Less than 5 subscribers per location</p>
              </div>
              
              <div className="border-l-4 border-gray-500 pl-4">
                <div className="flex items-center gap-2 mb-1">
                  <AlertTriangle className="h-4 w-4 text-gray-600" />
                  <h4 className="font-semibold text-gray-700">Frozen Account Status</h4>
                </div>
                <p className="text-sm text-gray-600">Account is Frozen</p>
              </div>
            </div>
            
            <div className="space-y-4">
              <div className="border-l-4 border-purple-500 pl-4">
                <div className="flex items-center gap-2 mb-1">
                  <DollarSign className="h-4 w-4 text-purple-500" />
                  <h4 className="font-semibold text-purple-700">Spend Drop</h4>
                </div>
                <p className="text-sm text-gray-600">Current spend is 50%+ lower than previous month</p>
              </div>
              
              <div className="border-l-4 border-blue-500 pl-4">
                <div className="flex items-center gap-2 mb-1">
                  <TrendingDown className="h-4 w-4 text-blue-500" />
                  <h4 className="font-semibold text-blue-700">Redemptions Drop</h4>
                </div>
                <p className="text-sm text-gray-600">Current redemptions are 50%+ lower than previous month</p>
              </div>
              
              <div className="border-l-4 border-red-500 pl-4">
                <div className="flex items-center gap-2 mb-1">
                  <AlertTriangle className="h-4 w-4 text-red-600" />
                  <h4 className="font-semibold text-red-700">Frozen & Inactive</h4>
                </div>
                <p className="text-sm text-gray-600">Account is Frozen and 1+ month since last text</p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Methodology */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5" />
            Scoring Methodology
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {/* Combined Calculation Processes - Spans 2 Columns */}
            <div className="md:col-span-2">
              <div className="bg-gradient-to-br from-indigo-50 to-purple-50 border border-indigo-200 rounded-lg p-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div>
                    <h4 className="font-semibold mb-3 text-indigo-800">Calculation Process—Launched Accounts</h4>
                    <ol className="list-decimal list-inside space-y-1 text-sm text-gray-700">
                      <li>Evaluate each account against all four risk flags</li>
                      <li>Count the number of active flags (0-4)</li>
                      <li>Assign risk level based on flag count</li>
                      <li>Update risk scores in real-time with new data</li>
                    </ol>
                  </div>
                  <div>
                    <h4 className="font-semibold mb-3 text-indigo-800">Calculation Process—Frozen Accounts</h4>
                    <ol className="list-decimal list-inside space-y-1 text-sm text-gray-700">
                      <li>Check account frozen status</li>
                      <li>Evaluate last text sent date</li>
                      <li>Assign Medium (frozen) or High (frozen + 1+ month inactive)</li>
                      <li>Update risk scores in real-time with new data</li>
                    </ol>
                  </div>
                </div>
              </div>
            </div>
            
            {/* Data Sources Column */}
            <div>
              <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                <h4 className="font-semibold mb-4 text-gray-800">Data Sources</h4>
                <ul className="list-disc list-inside space-y-2 text-sm text-gray-700">
                  <li>BigQuery analytics warehouse</li>
                  <li>Real-time transaction data</li>
                  <li>Subscriber activity metrics</li>
                  <li>Historical performance comparisons</li>
                  <li>Account status and messaging history</li>
                </ul>
              </div>
            </div>
          </div>
            
          <div className="mt-6 pt-4 border-t">
            <h4 className="font-semibold mb-3">Important Notes</h4>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm text-gray-600">
              <div className="bg-blue-50 p-3 rounded border border-blue-200">
                <strong className="text-blue-800">New Accounts:</strong> Only Monthly Redemptions and Low Activity flags are available for accounts in their first two months.
              </div>
              <div className="bg-orange-50 p-3 rounded border border-orange-200">
                <strong className="text-orange-800">Established Accounts:</strong> All four flags are evaluated for accounts operating for 2+ months.
              </div>
              <div className="bg-purple-50 p-3 rounded border border-purple-200">
                <strong className="text-purple-800">Frozen Status:</strong> Accounts with a Frozen status are assigned a risk level independently of other performance flags and can apply to both New and Established Accounts.
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      </div>

      {/* HubSpot Integration Section - Always visible outside collapsible area */}
      <div className="mt-8 space-y-6">
        {/* Main Title with Toggle */}
        <div className="text-center">
          <div className="flex items-center justify-center gap-4 mb-2">
            <h2 className="text-2xl font-bold text-gray-900">HubSpot Integration</h2>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setIsHubSpotOpen(!isHubSpotOpen)}
              className="flex items-center gap-2"
            >
              {isHubSpotOpen ? (
                <>
                  Hide Details <ChevronUp className="h-4 w-4" />
                </>
              ) : (
                <>
                  Show Details <ChevronDown className="h-4 w-4" />
                </>
              )}
            </Button>
          </div>
          <p className="text-gray-600">Automated daily sync of risk assessments to your HubSpot company records</p>
        </div>

        {/* Collapsible Content */}
        <div className={`space-y-6 transition-all duration-300 ${isHubSpotOpen ? 'block opacity-100' : 'hidden opacity-0'}`}>
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <svg className="h-5 w-5 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 1.79 4 4 4h8c2.21 0 4-1.79 4-4V7M4 7l2-2h12l2 2M4 7h16M10 11v6m4-6v6" />
                </svg>
                Automated Risk Data Sync
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                <div className="flex items-start gap-3">
                  <div className="flex-shrink-0">
                    <svg className="h-6 w-6 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                  </div>
                  <div className="flex-1">
                    <h4 className="font-medium text-blue-900 mb-2">HubSpot Company Properties Updated</h4>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                      <div className="bg-white/70 rounded-lg p-3 border border-blue-200">
                        <div className="font-medium text-blue-900 text-sm mb-1">ChurnGuard Current Risk Level</div>
                        <div className="text-xs text-blue-700">Dropdown values: Low, Medium, High</div>
                      </div>
                      <div className="bg-white/70 rounded-lg p-3 border border-blue-200">
                        <div className="font-medium text-blue-900 text-sm mb-1">ChurnGuard Current Risk Reasons</div>
                        <div className="text-xs text-blue-700">Specific flags and risk factors detected</div>
                      </div>
                      <div className="bg-white/70 rounded-lg p-3 border border-blue-200">
                        <div className="font-medium text-blue-900 text-sm mb-1">ChurnGuard Trending Risk Level</div>
                        <div className="text-xs text-blue-700">Dropdown values: Low, Medium, High</div>
                      </div>
                      <div className="bg-white/70 rounded-lg p-3 border border-blue-200">
                        <div className="font-medium text-blue-900 text-sm mb-1">ChurnGuard Trending Risk Reasons</div>
                        <div className="text-xs text-blue-700">Trending analysis of risk factor patterns</div>
                      </div>
                      <div className="bg-white/70 rounded-lg p-3 border border-blue-200">
                        <div className="font-medium text-blue-900 text-sm mb-1">ChurnGuard Last Updated</div>
                        <div className="text-xs text-blue-700">Date when ChurnGuard data was last synced</div>
                      </div>
                    </div>
                    <div className="mt-4 pt-3 border-t border-blue-200">
                      <div className="flex items-center justify-between">
                        <div>
                          <p className="text-sm font-medium text-blue-800 mb-1">Sync Schedule</p>
                          <p className="text-xs text-blue-600">Daily at 2:10 AM • Last sync: Today</p>
                        </div>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={handleHubSpotSync}
                          disabled={hubSpotLoading}
                          className="text-blue-700 border-blue-300 hover:bg-blue-50"
                        >
                          {hubSpotLoading ? (
                            <>
                              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                              Syncing...
                            </>
                          ) : (
                            <>
                              <RefreshCw className="h-4 w-4 mr-2" />
                              Sync Now
                            </>
                          )}
                        </Button>
                      </div>
                    </div>
                    <p className="text-xs text-blue-600 mt-3 italic">
                      Contact your admin to configure the HubSpot API connection for your organization.
                    </p>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}