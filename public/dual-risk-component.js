<<<<<<< HEAD
// Dual Risk Component - Updates Summary Cards with Dual Values
console.log('🎯 DualRiskComponent: Loading dual risk values...');

async function updateRiskSummaryCards() {
  try {
    console.log('📊 Fetching dual risk data from summary cards endpoint...');
    
    // Fetch from dedicated summary cards endpoint
    const response = await fetch('/api/summary-cards');
    const data = await response.json();
    
    if (!data || data.error) {
      console.error('❌ Error from summary cards API:', data?.error);
      return;
    }
    
    console.log('✅ Received dual risk data:', data);
    
    // Update High Risk card
    updateRiskCard('High Risk', data.high_risk.current, data.high_risk.trending);
    
    // Update Medium Risk card  
    updateRiskCard('Medium Risk', data.medium_risk.current, data.medium_risk.trending);
    
    // Update Low Risk card
    updateRiskCard('Low Risk', data.low_risk.current, data.low_risk.trending);
    
    console.log('🎉 Successfully updated all risk summary cards');
    
  } catch (error) {
    console.error('❌ Error updating risk summary cards:', error);
  }
}

function updateRiskCard(cardTitle, currentValue, trendingValue) {
  try {
    // Find the risk card by its title
    const cardElements = document.querySelectorAll('[class*="card"], [class*="metric"]');
    
    for (let card of cardElements) {
      const titleElement = card.querySelector('h3, h4, .font-semibold, [class*="title"]');
      
      if (titleElement && titleElement.textContent.includes(cardTitle)) {
        console.log(`📋 Updating ${cardTitle} card...`);
        
        // Find the value display element (usually the largest number)
        const valueElements = card.querySelectorAll('[class*="text-"], .text-lg, .text-xl, .text-2xl');
        
        for (let valueEl of valueElements) {
          // Look for numeric content that's not a percentage
          if (/^\d{1,4}$/.test(valueEl.textContent.trim())) {
            // Update with dual values
            valueEl.innerHTML = `
              <div style="display: flex; flex-direction: column; align-items: center; gap: 4px;">
                <div style="display: flex; justify-content: space-between; width: 100%; font-size: 0.85em; color: #6b7280;">
                  <span>Current</span>
                  <span>Trending</span>
                </div>
                <div style="display: flex; justify-content: space-between; width: 100%; font-weight: bold;">
                  <span>${currentValue}</span>
                  <span>${trendingValue}</span>
                </div>
              </div>
            `;
            
            console.log(`  ✅ Updated ${cardTitle}: Current ${currentValue}, Trending ${trendingValue}`);
            return;
          }
        }
      }
    }
    
    console.warn(`⚠️ Could not find ${cardTitle} card to update`);
  } catch (error) {
    console.error(`❌ Error updating ${cardTitle} card:`, error);
  }
}

// Run when page loads
document.addEventListener('DOMContentLoaded', () => {
  console.log('📅 DOM loaded, scheduling risk summary card updates...');
  setTimeout(updateRiskSummaryCards, 2000);
  setTimeout(updateRiskSummaryCards, 5000);
  setTimeout(updateRiskSummaryCards, 8000);
});

// Also run when page becomes visible
document.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    setTimeout(updateRiskSummaryCards, 1000);
  }
});

// Keep trying periodically until cards are updated
let updateInterval = setInterval(() => {
  const cards = document.querySelectorAll('[class*="card"], [class*="metric"]');
  if (cards.length > 0) {
    updateRiskSummaryCards();
  }
}, 10000); // Every 10 seconds
=======
// Dual Risk Component - Morning working state placeholder
// This file is referenced by index.html but will need to be recreated based on working implementation
console.log('🎯 DualRiskComponent: Loaded but not implemented yet');

// According to README, this component should:
// 1. Fetch from /api/bigquery/monthly-trends for trending data  
// 2. Fetch from /api/risk-scores/latest for historical data
// 3. Replace hard-coded values (559, 286, 28) with live data
// 4. Show dual-line display: "Risk Level" and "Trending Risk Level"
>>>>>>> e8c26b2069660ce67e204e9fc8fca2870c793650
