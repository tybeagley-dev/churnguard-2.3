# ChurnGuard 2.3 Google OAuth + Role-Based Dashboard Plan

## Phase 1: Google OAuth Foundation (Week 1)

### 1.1 Google Cloud Setup
- Create OAuth 2.0 client in Google Cloud Console
- Configure authorized domains and redirect URIs
- Set up environment variables for client credentials

### 1.2 Backend Authentication
- Install dependencies: `google-auth-library`, `jsonwebtoken`, `express-rate-limit`
- Create Google OAuth verification middleware
- Implement JWT token generation with user role embedding
- Add user profile storage in database

### 1.3 Database Schema
```sql
-- Users table for profile storage
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  name TEXT,
  picture_url TEXT,
  role TEXT, -- 'csm', 'leadership', 'external'
  csm_accounts TEXT[], -- Array of account IDs for CSMs
  created_at TIMESTAMP,
  last_login TIMESTAMP
);

-- Activity logging table
CREATE TABLE user_activity_logs (
  id SERIAL PRIMARY KEY,
  user_email TEXT,
  user_role TEXT,
  session_id TEXT,
  action_type TEXT,
  resource_accessed TEXT,
  timestamp TIMESTAMP,
  duration_seconds INTEGER,
  metadata JSONB
);
```

### 1.4 Frontend Auth Integration
- Replace current auth hook with Google OAuth flow
- Update login form with "Sign in with Google" button
- Handle OAuth callback and token storage

## Phase 2: Role-Based Dashboard (Week 2)

### 2.1 Role Detection System
- Create role mapping service (email → role + account assignments)
- Build CSM account assignment logic (link emails to account_ids)
- Implement leadership whitelist configuration

### 2.2 Dynamic Layout Components
```tsx
// New layout wrapper
<RoleBasedDashboard userRole={user.role}>
  <CSMDashboard accounts={user.csmAccounts} />
  <LeadershipDashboard />
</RoleBasedDashboard>
```

### 2.3 CSM Experience Implementation
- Auto-filter Account Metrics Overview to user's assigned accounts
- Reorder dashboard sections (accounts first, trends last)
- Add quick action buttons for CSM workflows
- Implement collapsible company-wide sections

### 2.4 Leadership Experience
- Maintain current layout (trends first, accounts last)
- Add advanced filtering and export capabilities
- Implement cross-account analytics views

## Phase 3: Usage Analytics & Admin Features (Week 3)

### 3.1 Activity Tracking System
- Page view tracking middleware
- Component interaction logging
- Session duration tracking
- Export/download activity monitoring

### 3.2 Analytics Dashboard (Admin Only)
- User activity reports
- Feature usage heatmaps
- Login frequency analytics
- Dashboard engagement metrics

### 3.3 User Management Interface
- Admin panel for role assignments
- CSM account mapping interface
- User access control management

## Technical Implementation Details

### Dependencies Required
```json
{
  "google-auth-library": "^8.9.0",
  "jsonwebtoken": "^9.0.0",
  "express-rate-limit": "^6.7.0",
  "@google-cloud/logging": "^10.5.0"
}
```

### Environment Variables
```env
GOOGLE_OAUTH_CLIENT_ID=your_client_id
GOOGLE_OAUTH_CLIENT_SECRET=your_secret
JWT_SECRET=your_jwt_secret
AUTHORIZED_DOMAINS=boostly.com,partnerdomain.com
```

### Security Features
- Email domain validation
- JWT token expiration (24h with refresh)
- Rate limiting on auth endpoints
- Audit logging for sensitive actions

### API Endpoints
```
POST /api/auth/google - Google OAuth verification
GET /api/auth/profile - Get user profile & role
POST /api/auth/refresh - Refresh JWT token
GET /api/admin/analytics - Usage analytics (admin only)
POST /api/admin/assign-role - User role management
```

### Role Configuration
```javascript
const roleConfig = {
  csm: {
    layout: 'accounts-first',
    permissions: ['view-assigned-accounts', 'export-data'],
    autoFilter: true
  },
  leadership: {
    layout: 'trends-first',
    permissions: ['view-all-accounts', 'admin-analytics'],
    autoFilter: false
  }
};
```

## Role-Based UX Strategy

### CSM Experience
```
┌─ Account Metrics Overview (TOP) ─┐
│  Auto-filtered to their accounts  │
│  Quick action buttons            │
└──────────────────────────────────┘
┌─ Their Account Performance ──────┐
│  Focused metrics & alerts        │
└──────────────────────────────────┘
┌─ Historical/Trends (BOTTOM) ─────┐
│  Company-wide context (collapsed)│
└──────────────────────────────────┘
```

### Leadership Experience
```
┌─ Historical Performance (TOP) ───┐
│  Company-wide trends & insights  │
└──────────────────────────────────┘
┌─ Monthly Trends ─────────────────┐
│  Strategic overview              │
└──────────────────────────────────┘
┌─ All Accounts List (BOTTOM) ─────┐
│  Unfiltered, sortable           │
└──────────────────────────────────┘
```

## Success Metrics
- Seamless OAuth login flow (< 3 clicks)
- Role-appropriate dashboard loading (< 2s)
- Activity logging capture rate (> 95%)
- User satisfaction with personalized experience

## Business Benefits
- **Security**: Enterprise-grade Google OAuth eliminates password management
- **UX**: Role-tailored interfaces improve workflow efficiency
- **Analytics**: Usage tracking enables data-driven UX decisions
- **Scalability**: Automated role assignment supports team growth

---
*Plan created: September 26, 2025*
*Status: Ready for implementation when prioritized*