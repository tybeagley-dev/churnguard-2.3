import Navbar from "@/components/layout/navbar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { PasswordSettings } from "@/components/settings/password-settings";

export default function Settings() {
  return (
    <div className="min-h-screen bg-gray-50">
      <Navbar />
      
      <main className="p-6">
        <div className="max-w-4xl mx-auto">
          <h1 className="text-2xl font-semibold text-gray-900 mb-6">Settings</h1>
          
          <div className="grid gap-6">
            {/* Password Settings */}
            <PasswordSettings />
            
            <Card>
              <CardHeader>
                <CardTitle>Notification Preferences</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-gray-600">Notification and alert settings will be available here.</p>
              </CardContent>
            </Card>
            
            <Card>
              <CardHeader>
                <CardTitle>Data & Privacy</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-gray-600">Data management and privacy controls will be available here.</p>
              </CardContent>
            </Card>
          </div>
        </div>
      </main>
      
      <footer className="text-center py-4 text-xs text-purple-800 border-t">
        Certified Bonesaw Product 🪚
      </footer>
    </div>
  );
}