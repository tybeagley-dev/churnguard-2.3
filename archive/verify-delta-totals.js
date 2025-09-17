// Delta Verification Script
// Verifies that summary card deltas match the sum of individual account deltas

import fetch from 'node-fetch';

async function verifyDeltaTotals() {
  try {
    console.log('🔍 Verifying delta totals consistency...\n');
    
    // Fetch current month data (883 accounts)
    const currentResponse = await fetch('http://localhost:3002/api/bigquery/accounts/monthly?period=current_month');
    const currentMonthData = await currentResponse.json();
    
    // Fetch previous month data (880 accounts)  
    const previousResponse = await fetch('http://localhost:3002/api/bigquery/accounts/monthly?period=previous_month');
    const previousMonthData = await previousResponse.json();
    
    console.log(`📊 Current month accounts: ${currentMonthData.length}`);
    console.log(`📊 Previous month accounts: ${previousMonthData.length}\n`);
    
    // Calculate summary totals for current month
    const currentTotals = currentMonthData.reduce((acc, account) => {
      acc.totalSpend += account.total_spend || 0;
      acc.totalTexts += account.total_texts_delivered || 0;
      acc.totalRedemptions += account.coupons_redeemed || 0;
      acc.totalSubscribers += account.active_subs_cnt || 0;
      return acc;
    }, { totalSpend: 0, totalTexts: 0, totalRedemptions: 0, totalSubscribers: 0 });
    
    // Calculate summary totals for previous month
    const previousTotals = previousMonthData.reduce((acc, account) => {
      acc.totalSpend += account.total_spend || 0;
      acc.totalTexts += account.total_texts_delivered || 0;
      acc.totalRedemptions += account.coupons_redeemed || 0;
      acc.totalSubscribers += account.active_subs_cnt || 0;
      return acc;
    }, { totalSpend: 0, totalTexts: 0, totalRedemptions: 0, totalSubscribers: 0 });
    
    // Calculate expected summary deltas
    const expectedSummaryDeltas = {
      spendDelta: currentTotals.totalSpend - previousTotals.totalSpend,
      textsDelta: currentTotals.totalTexts - previousTotals.totalTexts,
      redemptionsDelta: currentTotals.totalRedemptions - previousTotals.totalRedemptions,
      subscribersDelta: currentTotals.totalSubscribers - previousTotals.totalSubscribers
    };
    
    console.log('💳 Expected Summary Card Deltas:');
    console.log(`   Spend: $${expectedSummaryDeltas.spendDelta.toLocaleString()}`);
    console.log(`   Texts: ${expectedSummaryDeltas.textsDelta.toLocaleString()}`);
    console.log(`   Redemptions: ${expectedSummaryDeltas.redemptionsDelta.toLocaleString()}`);
    console.log(`   Subscribers: ${expectedSummaryDeltas.subscribersDelta.toLocaleString()}\n`);
    
    // Calculate individual account deltas (sum of all table row deltas)
    let accountDeltaTotals = { spendDelta: 0, textsDelta: 0, redemptionsDelta: 0, subscribersDelta: 0 };
    let matchedAccounts = 0;
    let unmatchedPreviousAccounts = 0;
    let unmatchedCurrentAccounts = 0;
    
    // For each previous month account, find matching current month account and calculate delta
    previousMonthData.forEach(prevAccount => {
      const currentAccount = currentMonthData.find(curr => curr.account_id === prevAccount.account_id);
      
      if (currentAccount) {
        matchedAccounts++;
        accountDeltaTotals.spendDelta += (currentAccount.total_spend || 0) - (prevAccount.total_spend || 0);
        accountDeltaTotals.textsDelta += (currentAccount.total_texts_delivered || 0) - (prevAccount.total_texts_delivered || 0);
        accountDeltaTotals.redemptionsDelta += (currentAccount.coupons_redeemed || 0) - (prevAccount.coupons_redeemed || 0);
        accountDeltaTotals.subscribersDelta += (currentAccount.active_subs_cnt || 0) - (prevAccount.active_subs_cnt || 0);
      } else {
        unmatchedPreviousAccounts++;
      }
    });
    
    // Count current month accounts that don't exist in previous month
    currentMonthData.forEach(currAccount => {
      const previousAccount = previousMonthData.find(prev => prev.account_id === currAccount.account_id);
      if (!previousAccount) {
        unmatchedCurrentAccounts++;
      }
    });
    
    console.log('🧮 Individual Account Delta Totals (Table Sum):');
    console.log(`   Spend: $${accountDeltaTotals.spendDelta.toLocaleString()}`);
    console.log(`   Texts: ${accountDeltaTotals.textsDelta.toLocaleString()}`);
    console.log(`   Redemptions: ${accountDeltaTotals.redemptionsDelta.toLocaleString()}`);
    console.log(`   Subscribers: ${accountDeltaTotals.subscribersDelta.toLocaleString()}\n`);
    
    console.log('📊 Account Matching Analysis:');
    console.log(`   Matched accounts (in both periods): ${matchedAccounts}`);
    console.log(`   Previous month only accounts: ${unmatchedPreviousAccounts}`);
    console.log(`   Current month only accounts: ${unmatchedCurrentAccounts}\n`);
    
    // Verify deltas match
    const tolerance = 0.01; // Allow for floating point rounding
    const results = [];
    
    function checkMatch(metric, expected, actual) {
      const match = Math.abs(expected - actual) < tolerance;
      const status = match ? '✅' : '❌';
      const diff = actual - expected;
      
      results.push({ metric, expected, actual, match, diff });
      console.log(`${status} ${metric}:`);
      console.log(`   Expected: ${expected.toLocaleString()}`);
      console.log(`   Actual:   ${actual.toLocaleString()}`);
      if (!match) console.log(`   Difference: ${diff.toLocaleString()}`);
      console.log('');
    }
    
    console.log('🔍 VERIFICATION RESULTS:\n');
    
    checkMatch('Spend Delta', expectedSummaryDeltas.spendDelta, accountDeltaTotals.spendDelta);
    checkMatch('Texts Delta', expectedSummaryDeltas.textsDelta, accountDeltaTotals.textsDelta);
    checkMatch('Redemptions Delta', expectedSummaryDeltas.redemptionsDelta, accountDeltaTotals.redemptionsDelta);
    checkMatch('Subscribers Delta', expectedSummaryDeltas.subscribersDelta, accountDeltaTotals.subscribersDelta);
    
    const allMatch = results.every(r => r.match);
    
    if (allMatch) {
      console.log('🎉 SUCCESS: All summary card deltas match individual account delta totals!');
    } else {
      console.log('⚠️  WARNING: Delta mismatches detected. This indicates a calculation inconsistency.');
      console.log('\nPossible causes:');
      console.log('- Account filtering differences between summary and table calculations');
      console.log('- Missing account handling (accounts in one period but not the other)');
      console.log('- Data type conversion issues');
    }
    
    // Show sample mismatched accounts if any
    if (unmatchedPreviousAccounts > 0 || unmatchedCurrentAccounts > 0) {
      console.log('\n📋 Account Mismatch Details:');
      
      if (unmatchedCurrentAccounts > 0) {
        console.log('\nNew accounts in current month (not in previous month):');
        const newAccounts = currentMonthData.filter(curr => 
          !previousMonthData.find(prev => prev.account_id === curr.account_id)
        ).slice(0, 5); // Show first 5
        
        newAccounts.forEach(acc => {
          console.log(`   ${acc.account_id}: ${acc.name} - $${(acc.total_spend || 0).toLocaleString()}`);
        });
        if (unmatchedCurrentAccounts > 5) {
          console.log(`   ... and ${unmatchedCurrentAccounts - 5} more`);
        }
      }
      
      if (unmatchedPreviousAccounts > 0) {
        console.log('\nAccounts that churned/paused (in previous month but not current):');
        const churnedAccounts = previousMonthData.filter(prev => 
          !currentMonthData.find(curr => curr.account_id === prev.account_id)
        ).slice(0, 5); // Show first 5
        
        churnedAccounts.forEach(acc => {
          console.log(`   ${acc.account_id}: ${acc.name} - $${(acc.total_spend || 0).toLocaleString()}`);
        });
        if (unmatchedPreviousAccounts > 5) {
          console.log(`   ... and ${unmatchedPreviousAccounts - 5} more`);
        }
      }
    }
    
  } catch (error) {
    console.error('❌ Error verifying delta totals:', error.message);
  }
}

verifyDeltaTotals();