import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

export default function TrendChart() {
  // Mock data for 6-month trend
  const data = [
    { month: "Jan", lowRisk: 850, mediumRisk: 120, highRisk: 45 },
    { month: "Feb", lowRisk: 900, mediumRisk: 140, highRisk: 52 },
    { month: "Mar", lowRisk: 920, mediumRisk: 155, highRisk: 68 },
    { month: "Apr", lowRisk: 980, mediumRisk: 180, highRisk: 72 },
    { month: "May", lowRisk: 1020, mediumRisk: 165, highRisk: 85 },
    { month: "Jun", lowRisk: 1002, mediumRisk: 156, highRisk: 89 },
  ];

  return (
    <Card className="lg:col-span-2 shadow-sm border border-gray-200">
      <CardHeader>
        <CardTitle className="text-lg font-semibold text-gray-900">6-Month Risk Trend</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="lowRisk" stackId="a" fill="#10B981" />
              <Bar dataKey="mediumRisk" stackId="a" fill="#F59E0B" />
              <Bar dataKey="highRisk" stackId="a" fill="#EF4444" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
