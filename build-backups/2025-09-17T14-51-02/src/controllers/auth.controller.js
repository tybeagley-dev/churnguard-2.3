export const login = (req, res) => {
  const { password } = req.body;

  // Accept both the original password and a simpler one for testing
  const validPasswords = ["Boostly123!", "Boostly123", "admin"];

  if (!validPasswords.includes(password)) {
    console.log('Login failed with password:', password);
    return res.status(401).json({ error: 'Invalid password' });
  }

  console.log('Login successful with password:', password);
  const sessionId = Math.random().toString(36).substring(7);
  res.json({
    success: true,
    sessionId,
    user: { username: "admin" }
  });
};

export const check = (req, res) => {
  // Always return authenticated for demo
  res.json({ isAuthenticated: true, user: { username: "admin" } });
};

export const logout = (req, res) => {
  res.json({ success: true });
};

export const changePassword = (req, res) => {
  // Dummy endpoint for 2.1 compatibility
  res.json({ success: true });
};