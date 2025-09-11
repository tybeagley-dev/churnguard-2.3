import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { BarChart3, Settings, LogOut, Zap } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/hooks/use-auth";

const navigation = [
  { name: "Dashboard", href: "/", icon: BarChart3 },
  { name: "Settings", href: "/settings", icon: Settings },
];

export default function Navbar() {
  const [location] = useLocation();
  const { logout } = useAuth();

  const handleLogout = () => {
    logout();
  };

  return (
    <nav className="bg-white shadow-sm border-b border-gray-200">
      <div className="px-6 py-4">
        <div className="flex items-center justify-between">
          {/* Logo and Brand */}
          <div className="flex items-center space-x-3">
            <div className="w-8 h-8 rounded-lg flex items-center justify-center bg-[#0408e7]">
              <BarChart3 className="h-5 w-5 text-white" />
            </div>
            <h1 className="text-xl font-semibold text-gray-900">ChurnGuard</h1>
          </div>
          
          {/* Navigation Links and Logout - Right Justified */}
          <div className="flex items-center space-x-6">
            <div className="flex items-center space-x-8">
              {navigation.map((item) => {
                const Icon = item.icon;
                const isActive = location === item.href;
                
                return (
                  <Link key={item.name} href={item.href}>
                    <div
                      className={cn(
                        "flex items-center px-4 py-2 text-sm font-medium rounded-lg transition-colors cursor-pointer",
                        isActive
                          ? "text-primary bg-blue-50"
                          : "text-gray-700 hover:text-gray-900 hover:bg-gray-100"
                      )}
                    >
                      <Icon className="h-4 w-4 mr-2" />
                      {item.name}
                    </div>
                  </Link>
                );
              })}
            </div>
            
            {/* Logout Button */}
            <Button
              variant="ghost"
              size="sm"
              onClick={handleLogout}
              className="text-gray-700 hover:text-gray-900"
            >
              <LogOut className="h-4 w-4 mr-2" />
              Logout
            </Button>
          </div>
        </div>
      </div>
    </nav>
  );
}