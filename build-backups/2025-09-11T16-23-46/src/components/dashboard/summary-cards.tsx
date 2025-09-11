import { Card, CardContent } from "@/components/ui/card";
import { Users, AlertTriangle, AlertCircle, DollarSign } from "lucide-react";
import { useDashboardAnalytics } from "@/hooks/use-risk-data";

export default function SummaryCards() {
  const { data: analytics, isLoading } = useDashboardAnalytics();

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        {[...Array(4)].map((_, i) => (
          <Card key={i} className="animate-pulse">
            <CardContent className="p-6">
              <div className="h-20 bg-gray-200 rounded"></div>
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  if (!analytics) {
    return null;
  }

  const cards = [
    {
      title: "Total Accounts",
      value: analytics.totalAccounts.toLocaleString(),
      icon: Users,
      iconBg: "bg-blue-100",
      iconColor: "text-primary",
      change: "+12%",
      changeColor: "text-green-600",
    },
    {
      title: "High Risk",
      value: analytics.highRiskCount.toString(),
      icon: AlertTriangle,
      iconBg: "bg-red-100",
      iconColor: "text-red-500",
      change: "+5%",
      changeColor: "text-red-600",
    },
    {
      title: "Medium Risk",
      value: analytics.mediumRiskCount.toString(),
      icon: AlertCircle,
      iconBg: "bg-amber-100",
      iconColor: "text-amber-500",
      change: "-2%",
      changeColor: "text-amber-600",
    },
    {
      title: "Revenue at Risk",
      value: `$${(analytics.revenueAtRisk / 1000).toFixed(0)}K`,
      icon: DollarSign,
      iconBg: "bg-green-100",
      iconColor: "text-green-500",
      change: "-8%",
      changeColor: "text-green-600",
    },
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
      {cards.map((card, index) => {
        const Icon = card.icon;
        
        return (
          <Card key={index} className="shadow-sm border border-gray-200">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600">{card.title}</p>
                  <p className="text-3xl font-bold text-gray-900">{card.value}</p>
                </div>
                <div className={`w-12 h-12 ${card.iconBg} rounded-lg flex items-center justify-center`}>
                  <Icon className={`${card.iconColor} h-6 w-6`} />
                </div>
              </div>
              <div className="flex items-center mt-4">
                <span className={`text-sm ${card.changeColor} font-medium`}>
                  {card.change}
                </span>
                <span className="text-sm text-gray-500 ml-2">
                  from last {index === 0 ? 'month' : 'week'}
                </span>
              </div>
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}
