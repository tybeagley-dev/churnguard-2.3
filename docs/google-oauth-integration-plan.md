# Google OAuth Integration Plan for ChurnGuard 2.3

## Executive Summary

This document outlines the strategy for implementing Google OAuth authentication in ChurnGuard 2.3, including user management, account association, and security considerations. The plan prioritizes implementation on the production PostgreSQL environment to avoid migration complexity.

## Current Authentication State

### Existing Infrastructure
- ✅ Express.js backend with modular auth routes (`/api/auth/*`)
- ✅ React frontend with auth context and hooks (`useAuth`)
- ✅ Token-based session management via localStorage
- ✅ Clean separation between auth logic and business logic

### Current Limitations
- Single-user system with hardcoded password authentication
- No user-account association or data scoping
- No external authentication providers

## Implementation Strategy

### Phase 1: Database Schema Design

#### User Management Tables
```sql
-- Core user table for OAuth users
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    google_id TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    avatar_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

-- User-Account association with role-based access
CREATE TABLE user_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    access_level TEXT NOT NULL CHECK (access_level IN ('owner', 'manager', 'viewer')),
    granted_by UUID REFERENCES users(id),
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, account_id)
);

-- Admin override table for system administrators
CREATE TABLE admin_users (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    granted_by UUID REFERENCES users(id),
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Indexes for Performance
```sql
CREATE INDEX idx_user_accounts_user_id ON user_accounts(user_id);
CREATE INDEX idx_user_accounts_account_id ON user_accounts(account_id);
CREATE INDEX idx_users_google_id ON users(google_id);
CREATE INDEX idx_users_email ON users(email);
```

### Phase 2: Backend Implementation

#### Required Dependencies
```json
{
  "passport": "^0.7.0",
  "passport-google-oauth20": "^2.0.0",
  "express-session": "^1.17.3",
  "connect-session-sequelize": "^7.1.7",
  "dotenv": "^16.3.1"
}
```

#### OAuth Configuration
```javascript
// config/passport.js
import passport from 'passport';
import { Strategy as GoogleStrategy } from 'passport-google-oauth20';

passport.use(new GoogleStrategy({
  clientID: process.env.GOOGLE_CLIENT_ID,
  clientSecret: process.env.GOOGLE_CLIENT_SECRET,
  callbackURL: process.env.GOOGLE_CALLBACK_URL
}, async (accessToken, refreshToken, profile, done) => {
  // User lookup/creation logic
}));
```

#### New Auth Routes
```javascript
// src/routes/oauth.routes.js
router.get('/auth/google', passport.authenticate('google', {
  scope: ['profile', 'email']
}));

router.get('/auth/google/callback',
  passport.authenticate('google', { failureRedirect: '/login' }),
  (req, res) => {
    // Successful authentication redirect
    res.redirect('/dashboard');
  }
);
```

#### User Service Layer
```javascript
// src/services/user.service.js
export const userService = {
  async findOrCreateGoogleUser(profile) {
    // Check existing user by google_id
    // Create new user if not found
    // Return user with associated accounts
  },

  async getUserAccounts(userId) {
    // Return accounts user has access to
    // Include access level for each account
  },

  async hasAccountAccess(userId, accountId, requiredLevel = 'viewer') {
    // Check if user can access specific account
    // Validate access level meets requirements
  }
};
```

### Phase 3: Frontend Integration

#### Updated Auth Context
```typescript
// src/hooks/use-auth.tsx
interface User {
  id: string;
  email: string;
  name: string;
  avatarUrl?: string;
  accounts: UserAccount[];
  isAdmin: boolean;
}

interface UserAccount {
  accountId: string;
  businessName: string;
  accessLevel: 'owner' | 'manager' | 'viewer';
}
```

#### Google Sign-In Component
```typescript
// src/components/auth/google-signin-button.tsx
export function GoogleSignInButton() {
  const handleGoogleSignIn = () => {
    window.location.href = '/api/auth/google';
  };

  return (
    <Button onClick={handleGoogleSignIn} variant="outline">
      <GoogleIcon className="mr-2" />
      Sign in with Google
    </Button>
  );
}
```

#### Account Filtering Logic
```typescript
// src/hooks/use-account-access.tsx
export function useAccountAccess() {
  const { user } = useAuth();

  const hasAccountAccess = useCallback((accountId: string, level: AccessLevel = 'viewer') => {
    if (user?.isAdmin) return true;

    const userAccount = user?.accounts.find(acc => acc.accountId === accountId);
    if (!userAccount) return false;

    return checkAccessLevel(userAccount.accessLevel, level);
  }, [user]);

  return { hasAccountAccess, userAccounts: user?.accounts || [] };
}
```

### Phase 4: Security Implementation

#### CSRF Protection
```javascript
// Implement state parameter for OAuth flow
const state = crypto.randomBytes(32).toString('hex');
req.session.oauthState = state;

// Verify state in callback
if (req.query.state !== req.session.oauthState) {
  return res.status(400).send('Invalid state parameter');
}
```

#### Session Security
```javascript
// Secure session configuration
app.use(session({
  secret: process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === 'production',
    httpOnly: true,
    maxAge: 24 * 60 * 60 * 1000 // 24 hours
  }
}));
```

#### API Route Protection
```javascript
// src/middleware/auth.middleware.js
export const requireAuth = async (req, res, next) => {
  if (!req.user) {
    return res.status(401).json({ error: 'Authentication required' });
  }
  next();
};

export const requireAccountAccess = (requiredLevel = 'viewer') => {
  return async (req, res, next) => {
    const { accountId } = req.params;
    const hasAccess = await userService.hasAccountAccess(
      req.user.id,
      accountId,
      requiredLevel
    );

    if (!hasAccess) {
      return res.status(403).json({ error: 'Insufficient permissions' });
    }
    next();
  };
};
```

## Access Control Design

### Permission Levels

#### Viewer
- Read-only access to dashboards and reports
- Can view account metrics and trends
- Cannot modify settings or configurations

#### Manager
- All viewer permissions
- Can update account settings
- Can view and modify risk thresholds
- Cannot manage user access

#### Owner
- All manager permissions
- Can grant/revoke access to other users
- Can delete accounts
- Full administrative control over account

#### System Admin
- Access to all accounts regardless of association
- User management capabilities
- System configuration access

### Data Scoping Strategy

#### Account-Level Filtering
```sql
-- Example: Get monthly trends for user's accounts only
SELECT mt.*
FROM monthly_trends mt
JOIN user_accounts ua ON mt.account_id = ua.account_id
WHERE ua.user_id = $1
  AND ua.access_level IN ('owner', 'manager', 'viewer')
```

#### Service Layer Implementation
```javascript
// All data services check user permissions
export const monthlyTrendsService = {
  async getTrendsForUser(userId, filters = {}) {
    const userAccounts = await userService.getUserAccounts(userId);
    const accountIds = userAccounts.map(acc => acc.accountId);

    return getTrends({
      ...filters,
      accountIds // Automatically scope to user's accounts
    });
  }
};
```

## Implementation Timing Strategy

### Recommendation: Implement on PostgreSQL Production Environment

#### Rationale
1. **Infrastructure Alignment**: OAuth is foundational infrastructure - build on production architecture from start
2. **Database Features**: PostgreSQL UUID types, better session handling, and superior OAuth tooling
3. **Production Testing**: OAuth callback URLs require real domain testing anyway
4. **Migration Avoidance**: Prevents complex user data migration from SQLite to PostgreSQL

#### Timeline Impact
- **Additional upfront time**: 2-3 days for production environment setup
- **Time saved**: 1-2 weeks avoiding migration complexity
- **Overall efficiency**: 85% probability of faster delivery

#### Prerequisites
- Render deployment with PostgreSQL database
- Production domain with SSL certificate
- Google Cloud Console OAuth application configured
- Environment variables properly configured

## Environment Configuration

### Required Environment Variables
```bash
# Google OAuth Configuration
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret
GOOGLE_CALLBACK_URL=https://yourdomain.com/api/auth/google/callback

# Session Security
SESSION_SECRET=your_secure_random_session_secret

# Database Configuration (PostgreSQL)
DATABASE_URL=postgresql://username:password@host:port/database
```

### Google Cloud Console Setup
1. Create new project or use existing
2. Enable Google+ API
3. Configure OAuth consent screen
4. Create OAuth 2.0 client credentials
5. Add authorized redirect URIs

## Migration Strategy

### From Current System
1. **Export existing account data** from SQLite
2. **Migrate core business data** to PostgreSQL
3. **Create initial admin user** via OAuth
4. **Associate admin user** with all existing accounts
5. **Test complete OAuth flow** before removing password auth

### User Onboarding Process
1. **Invitation system**: Existing users invite new users
2. **Account association**: Inviter grants appropriate access level
3. **Self-service**: New users sign in with Google, request access
4. **Admin approval**: Admin users can grant access to any account

## Testing Strategy

### Unit Tests
- User service functions (create, find, permissions)
- Access control middleware
- OAuth flow handlers

### Integration Tests
- Complete OAuth flow end-to-end
- Permission checking across all API endpoints
- Session management and expiry

### Security Tests
- CSRF protection validation
- Session hijacking prevention
- Permission escalation attempts
- SQL injection in user queries

## Performance Considerations

### Database Optimization
- Proper indexing on user-account relationships
- Connection pooling for OAuth sessions
- Query optimization for permission checks

### Caching Strategy
- Cache user permissions in session
- Redis cache for frequently accessed user data
- Invalidation strategy for permission changes

### Monitoring
- OAuth flow success/failure rates
- User session duration analytics
- Permission check performance metrics

## Rollback Plan

### Immediate Rollback
- Feature flag to disable OAuth and revert to password auth
- Database state preserved (OAuth tables don't affect existing data)
- Session cleanup and redirect to password login

### Data Recovery
- All business data remains unchanged
- User associations can be recreated if needed
- Admin access maintained through emergency backdoor

## Complexity Assessment

**Overall Complexity**: 6/10 (Moderate)

**Time Estimates**:
- Backend OAuth implementation: 6-8 hours
- Frontend integration: 4-6 hours
- User management and permissions: 4-6 hours
- Security hardening and testing: 4-6 hours
- **Total estimated time**: 18-26 hours

**Risk Factors**:
- OAuth callback configuration complexity
- Production environment dependencies
- User data association migration
- Security implementation correctness

## Future Enhancements

### Multi-Tenant Architecture
- Organization-level grouping above individual accounts
- Hierarchical permission inheritance
- Billing and subscription management per organization

### Additional OAuth Providers
- Microsoft Azure AD for enterprise customers
- GitHub OAuth for developer teams
- SAML integration for enterprise SSO

### Advanced Access Control
- Time-based access grants
- IP-based access restrictions
- Audit logging for all permission changes
- Role-based access control (RBAC) expansion

---

*Document created: 2025-09-19*
*Status: Planning Phase*
*Target Implementation: Post-PostgreSQL Migration*