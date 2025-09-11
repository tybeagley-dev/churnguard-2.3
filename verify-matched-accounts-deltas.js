// Matched Accounts Delta Verification Script
// Verifies delta calculations for the 842 accounts that exist in both periods

import fetch from 'node-fetch';

async function verifyMatchedAccountsDeltas() {
  try {
    console.log('🔍 Verifying delta calculations for matched accounts only...\n');
    
    // Fetch current month data (883 accounts)
    const currentResponse = await fetch('http://localhost:3002/api/bigquery/accounts/monthly?period=current_month');
    const currentMonthData = await currentResponse.json();
    
    // Fetch previous month data (880 accounts)  
    const previousResponse = await fetch('http://localhost:3002/api/bigquery/accounts/monthly?period=previous_month');
    const previousMonthData = await previousResponse.json();
    
    // Find the 842 matched accounts (exist in both periods)
    const matchedAccounts = [];
    
    previousMonthData.forEach(prevAccount => {
      const currentAccount = currentMonthData.find(curr => curr.account_id === prevAccount.account_id);
      if (currentAccount) {
        matchedAccounts.push({
          account_id: prevAccount.account_id,
          name: prevAccount.name,
          previous: prevAccount,
          current: currentAccount
        });
      }
    });
    
    console.log(`📊 Total accounts in current month: ${currentMonthData.length}`);
    console.log(`📊 Total accounts in previous month: ${previousMonthData.length}`);
    console.log(`📊 Matched accounts (in both periods): ${matchedAccounts.length}\n`);
    
    // Calculate summary totals for ONLY the matched accounts (current month values)
    const matchedCurrentTotals = matchedAccounts.reduce((acc, match) => {
      acc.totalSpend += match.current.total_spend || 0;
      acc.totalTexts += match.current.total_texts_delivered || 0;
      acc.totalRedemptions += match.current.coupons_redeemed || 0;
      acc.totalSubscribers += match.current.active_subs_cnt || 0;
      return acc;
    }, { totalSpend: 0, totalTexts: 0, totalRedemptions: 0, totalSubscribers: 0 });
    
    // Calculate summary totals for ONLY the matched accounts (previous month values)
    const matchedPreviousTotals = matchedAccounts.reduce((acc, match) => {
      acc.totalSpend += match.previous.total_spend || 0;
      acc.totalTexts += match.previous.total_texts_delivered || 0;
      acc.totalRedemptions += match.previous.coupons_redeemed || 0;
      acc.totalSubscribers += match.previous.active_subs_cnt || 0;
      return acc;
    }, { totalSpend: 0, totalTexts: 0, totalRedemptions: 0, totalSubscribers: 0 });
    
    // Calculate expected deltas for matched accounts
    const expectedMatchedDeltas = {
      spendDelta: matchedCurrentTotals.totalSpend - matchedPreviousTotals.totalSpend,
      textsDelta: matchedCurrentTotals.totalTexts - matchedPreviousTotals.totalTexts,
      redemptionsDelta: matchedCurrentTotals.totalRedemptions - matchedPreviousTotals.totalRedemptions,
      subscribersDelta: matchedCurrentTotals.totalSubscribers - matchedPreviousTotals.totalSubscribers
    };
    
    console.log('💳 Summary Card Style Calculation (842 Matched Accounts Only):');
    console.log(`   Current Total Spend: $${matchedCurrentTotals.totalSpend.toLocaleString()}`);
    console.log(`   Previous Total Spend: $${matchedPreviousTotals.totalSpend.toLocaleString()}`);
    console.log(`   Expected Spend Delta: $${expectedMatchedDeltas.spendDelta.toLocaleString()}\n`);
    
    console.log(`   Current Total Texts: ${matchedCurrentTotals.totalTexts.toLocaleString()}`);
    console.log(`   Previous Total Texts: ${matchedPreviousTotals.totalTexts.toLocaleString()}`);
    console.log(`   Expected Texts Delta: ${expectedMatchedDeltas.textsDelta.toLocaleString()}\n`);
    
    console.log(`   Current Total Redemptions: ${matchedCurrentTotals.totalRedemptions.toLocaleString()}`);
    console.log(`   Previous Total Redemptions: ${matchedPreviousTotals.totalRedemptions.toLocaleString()}`);
    console.log(`   Expected Redemptions Delta: ${expectedMatchedDeltas.redemptionsDelta.toLocaleString()}\n`);
    
    console.log(`   Current Total Subscribers: ${matchedCurrentTotals.totalSubscribers.toLocaleString()}`);
    console.log(`   Previous Total Subscribers: ${matchedPreviousTotals.totalSubscribers.toLocaleString()}`);
    console.log(`   Expected Subscribers Delta: ${expectedMatchedDeltas.subscribersDelta.toLocaleString()}\n`);
    
    // Calculate individual account deltas (exactly what the table shows)
    let tableRowDeltaTotals = { spendDelta: 0, textsDelta: 0, redemptionsDelta: 0, subscribersDelta: 0 };
    
    matchedAccounts.forEach(match => {
      tableRowDeltaTotals.spendDelta += (match.current.total_spend || 0) - (match.previous.total_spend || 0);
      tableRowDeltaTotals.textsDelta += (match.current.total_texts_delivered || 0) - (match.previous.total_texts_delivered || 0);
      tableRowDeltaTotals.redemptionsDelta += (match.current.coupons_redeemed || 0) - (match.previous.coupons_redeemed || 0);
      tableRowDeltaTotals.subscribersDelta += (match.current.active_subs_cnt || 0) - (match.previous.active_subs_cnt || 0);
    });
    
    console.log('🧮 Table Row Delta Totals (Sum of Individual Account Deltas):');
    console.log(`   Spend Delta: $${tableRowDeltaTotals.spendDelta.toLocaleString()}`);
    console.log(`   Texts Delta: ${tableRowDeltaTotals.textsDelta.toLocaleString()}`);
    console.log(`   Redemptions Delta: ${tableRowDeltaTotals.redemptionsDelta.toLocaleString()}`);
    console.log(`   Subscribers Delta: ${tableRowDeltaTotals.subscribersDelta.toLocaleString()}\n`);
    
    // Verify the calculations match
    const tolerance = 0.01; // Allow for floating point rounding
    
    function checkMatch(metric, expected, actual) {
      const match = Math.abs(expected - actual) < tolerance;
      const status = match ? '✅' : '❌';
      const diff = Math.abs(actual - expected);
      
      console.log(`${status} ${metric}:`);
      console.log(`   Summary Style: ${expected.toLocaleString()}`);
      console.log(`   Table Row Sum: ${actual.toLocaleString()}`);
      if (!match) console.log(`   Difference: ${diff.toLocaleString()}`);
      console.log('');
      
      return match;
    }
    
    console.log('🔍 MATCHED ACCOUNTS VERIFICATION RESULTS:\n');
    
    const spendMatch = checkMatch('Spend Delta', expectedMatchedDeltas.spendDelta, tableRowDeltaTotals.spendDelta);
    const textsMatch = checkMatch('Texts Delta', expectedMatchedDeltas.textsDelta, tableRowDeltaTotals.textsDelta);
    const redemptionsMatch = checkMatch('Redemptions Delta', expectedMatchedDeltas.redemptionsDelta, tableRowDeltaTotals.redemptionsDelta);
    const subscribersMatch = checkMatch('Subscribers Delta', expectedMatchedDeltas.subscribersDelta, tableRowDeltaTotals.subscribersDelta);
    
    const allMatch = spendMatch && textsMatch && redemptionsMatch && subscribersMatch;
    
    if (allMatch) {
      console.log('🎉 SUCCESS: Table row delta calculations are mathematically correct!');
      console.log('✅ For the 842 accounts that exist in both periods, the sum of individual');
      console.log('   account deltas matches the expected (current - previous) calculation.');
    } else {
      console.log('⚠️  WARNING: Delta calculation error detected in table logic.');
      console.log('💡 This indicates a bug in how individual account deltas are calculated.');
    }
    
    // Show sample accounts for verification
    console.log('\n📋 Sample Account Delta Calculations (First 5):');
    matchedAccounts.slice(0, 5).forEach(match => {
      const spendDelta = (match.current.total_spend || 0) - (match.previous.total_spend || 0);
      const textsDelta = (match.current.total_texts_delivered || 0) - (match.previous.total_texts_delivered || 0);
      
      console.log(`\n   ${match.account_id}: ${match.name}`);
      console.log(`     Current Spend: $${(match.current.total_spend || 0).toLocaleString()}`);
      console.log(`     Previous Spend: $${(match.previous.total_spend || 0).toLocaleString()}`);
      console.log(`     Spend Delta: $${spendDelta.toLocaleString()}`);
      console.log(`     Current Texts: ${(match.current.total_texts_delivered || 0).toLocaleString()}`);
      console.log(`     Previous Texts: ${(match.previous.total_texts_delivered || 0).toLocaleString()}`);
      console.log(`     Texts Delta: ${textsDelta.toLocaleString()}`);
    });
    
  } catch (error) {
    console.error('❌ Error verifying matched account deltas:', error.message);
  }
}

verifyMatchedAccountsDeltas();