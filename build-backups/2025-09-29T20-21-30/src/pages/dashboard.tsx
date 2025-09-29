import Navbar from "@/components/layout/navbar";
import DashboardTabs from "@/components/dashboard/dashboard-tabs";
import RiskScoringLegend from "@/components/dashboard/risk-scoring-legend";

export default function Dashboard() {
  return (
    <div className="min-h-screen bg-gray-50">
      <Navbar />
      
      <main className="flex-1 p-6">
        <DashboardTabs />
        <RiskScoringLegend />
      </main>
      
      <footer className="text-center py-4 text-xs text-purple-800 border-t">
        Certified Bonesaw Product 🪚
      </footer>
    </div>
  );
}
