import { createContext, useContext, useState, useEffect, ReactNode } from 'react';

interface User {
  id: string;
}

interface AuthContextType {
  user: User | null;
  isAuthenticated: boolean;
  login: (password: string) => Promise<void>;
  logout: () => void;
  error: string | null;
  isLoading: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Check authentication status on mount
  useEffect(() => {
    checkAuthStatus();
  }, []);

  const checkAuthStatus = async () => {
    try {
      const sessionId = localStorage.getItem('sessionId');
      if (!sessionId) {
        setIsLoading(false);
        return;
      }

      const response = await fetch('/api/auth/check', {
        headers: {
          'Authorization': `Bearer ${sessionId}`
        }
      });

      if (response.ok) {
        const data = await response.json();
        if (data.authenticated) {
          setUser(data.user);
        } else {
          localStorage.removeItem('sessionId');
        }
      } else {
        localStorage.removeItem('sessionId');
      }
    } catch (error) {
      console.error('Auth check failed:', error);
      localStorage.removeItem('sessionId');
    } finally {
      setIsLoading(false);
    }
  };

  const login = async (password: string) => {
    setError(null);
    setIsLoading(true);

    try {
      const response = await fetch('/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ password }),
      });

      const data = await response.json();

      if (response.ok) {
        localStorage.setItem('sessionId', data.sessionId);
        setUser({ id: 'admin' });
        setError(null);
      } else {
        setError(data.error || 'Login failed');
      }
    } catch (error) {
      setError('Connection error. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  const logout = async () => {
    try {
      const sessionId = localStorage.getItem('sessionId');
      if (sessionId) {
        await fetch('/api/auth/logout', {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${sessionId}`
          }
        });
      }
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      localStorage.removeItem('sessionId');
      setUser(null);
      setError(null);
    }
  };

  const value: AuthContextType = {
    user,
    isAuthenticated: !!user,
    login,
    logout,
    error,
    isLoading,
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}