import { useState } from "react";
import AccountMetricsTable from "./account-metrics-table";
import AccountMetricsTableMonthly from "./account-metrics-table-monthly";
import { Claude12MonthChart } from "./claude-12month-chart";

import MonthlyTrendsChart from "./monthly-trends-chart";

export default function DashboardTabs() {
  const [activeTab, setActiveTab] = useState("weekly");

  return (
    <div className="w-full">
      {/* Claude 12-month test Chart */}
      <div className="mb-6">
        <Claude12MonthChart />
      </div>
      

      {/* Monthly Trends Chart */}
      <div className="mb-6">
        <MonthlyTrendsChart />
      </div>
      
      {/* Custom folder-style tabs */}
      <div className="flex justify-start">
        <div className="flex">
          <button
            onClick={() => setActiveTab("weekly")}
            className={`px-6 py-3 rounded-t-lg border-t border-l border-r font-medium text-sm transition-colors relative ${
              activeTab === "weekly"
                ? "bg-white text-gray-900 border-gray-300 z-10"
                : "bg-gray-100 text-gray-600 border-gray-200 hover:text-gray-900 hover:bg-gray-50"
            }`}
            style={{ 
              borderBottom: activeTab === "weekly" ? "1px solid white" : "1px solid #d1d5db",
              marginBottom: "-1px"
            }}
          >
            Weekly View
          </button>
          <button
            onClick={() => setActiveTab("monthly")}
            className={`px-6 py-3 rounded-t-lg border-t border-l border-r font-medium text-sm transition-colors relative -ml-px ${
              activeTab === "monthly"
                ? "bg-white text-gray-900 border-gray-300 z-10"
                : "bg-gray-100 text-gray-600 border-gray-200 hover:text-gray-900 hover:bg-gray-50"
            }`}
            style={{ 
              borderBottom: activeTab === "monthly" ? "1px solid white" : "1px solid #d1d5db",
              marginBottom: "-1px"
            }}
          >
            Monthly View
          </button>
        </div>
      </div>
      
      {/* Tab content - directly connected to tabs */}
      <div className="bg-white border border-gray-300 rounded-lg rounded-tl-none -mt-px">
        {activeTab === "weekly" && <AccountMetricsTable />}
        {activeTab === "monthly" && <AccountMetricsTableMonthly />}
      </div>
    </div>
  );
}