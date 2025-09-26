export class ChurnGuardCalendar {

  static formatDateISO(date) {
    return date.toISOString().split('T')[0];
  }

  static formatMonthISO(date) {
    return date.toISOString().slice(0, 7);
  }

  static getDateInfo(inputDate = null) {
    const date = inputDate ? new Date(inputDate) : new Date();
    const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

    return {
      date: this.formatDateISO(date),
      dayOfWeek: dayNames[date.getDay()],
      dayOfWeekNumber: date.getDay(),

      week: this.getWeekContext(date),
      month: this.getMonthContext(date),
      comparisons: this.getComparisonContext(date)
    };
  }

  static getWeekContext(date) {
    const dayOfWeek = date.getDay();

    // Get Sunday of current week
    const weekStart = new Date(date);
    weekStart.setDate(date.getDate() - dayOfWeek);
    weekStart.setHours(0, 0, 0, 0);

    // Get yesterday (most recent complete day)
    const yesterday = new Date(date);
    yesterday.setDate(date.getDate() - 1);

    return {
      start: this.formatDateISO(weekStart),
      end: this.formatDateISO(yesterday),
      currentWeekStart: this.formatDateISO(weekStart)
    };
  }

  static getMonthContext(date) {
    const monthStart = new Date(date.getFullYear(), date.getMonth(), 1);
    const yesterday = new Date(date);
    yesterday.setDate(date.getDate() - 1);

    return {
      start: this.formatDateISO(monthStart),
      end: this.formatDateISO(yesterday),
      current: this.formatMonthISO(date),
      dayOfMonth: date.getDate(),
      daysInMonth: new Date(date.getFullYear(), date.getMonth() + 1, 0).getDate()
    };
  }

  static getComparisonContext(date) {
    return {
      previousWeek: this.getPreviousWeekComplete(date),
      sameWeekLastMonth: this.getSameWeekLastMonthComplete(date),
      sameWeekLastYear: this.getSameWeekLastYearComplete(date),
      sixWeekAverage: this.getSixWeekAverageComplete(date),
      previousMonth: this.getPreviousMonthComplete(date),
      sameMonthLastYear: this.getSameMonthLastYearComplete(date),
      threeMonthAverage: this.getThreeMonthAverageComplete(date)
    };
  }

  static getPreviousWeekComplete(date) {
    const dayOfWeek = date.getDay();

    // Previous week's Sunday
    const prevWeekStart = new Date(date);
    prevWeekStart.setDate(date.getDate() - dayOfWeek - 7);
    prevWeekStart.setHours(0, 0, 0, 0);

    // Previous week's Saturday
    const prevWeekEnd = new Date(prevWeekStart);
    prevWeekEnd.setDate(prevWeekStart.getDate() + 6);

    return {
      start: this.formatDateISO(prevWeekStart),
      end: this.formatDateISO(prevWeekEnd)
    };
  }

  static getSameWeekLastMonthComplete(date) {
    const dayOfWeek = date.getDay();

    // Go back 4 weeks (28 days)
    const lastMonthWeek = new Date(date);
    lastMonthWeek.setDate(date.getDate() - 28);

    // Find Sunday of that week
    const weekStart = new Date(lastMonthWeek);
    weekStart.setDate(lastMonthWeek.getDate() - lastMonthWeek.getDay());

    // Same day pattern as current week (yesterday relative to current day of week)
    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + dayOfWeek - 1);

    return {
      start: this.formatDateISO(weekStart),
      end: this.formatDateISO(weekEnd)
    };
  }

  static getSameWeekLastYearComplete(date) {
    const dayOfWeek = date.getDay();

    // Same date last year
    const lastYearWeek = new Date(date);
    lastYearWeek.setFullYear(date.getFullYear() - 1);

    // Find Sunday of that week
    const weekStart = new Date(lastYearWeek);
    weekStart.setDate(lastYearWeek.getDate() - lastYearWeek.getDay());

    // Same day pattern as current week
    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + dayOfWeek - 1);

    return {
      start: this.formatDateISO(weekStart),
      end: this.formatDateISO(weekEnd)
    };
  }

  static getSixWeekAverageComplete(date) {
    const dayOfWeek = date.getDay();

    // 6 weeks ago Sunday
    const sixWeeksAgo = new Date(date);
    sixWeeksAgo.setDate(date.getDate() - dayOfWeek - (6 * 7));

    // End of last week (Saturday)
    const lastWeekEnd = new Date(date);
    lastWeekEnd.setDate(date.getDate() - dayOfWeek - 1);

    return {
      start: this.formatDateISO(sixWeeksAgo),
      end: this.formatDateISO(lastWeekEnd)
    };
  }

  static getPreviousMonthComplete(date) {
    const currentDay = date.getDate();

    // First day of previous month
    const prevMonthStart = new Date(date.getFullYear(), date.getMonth() - 1, 1);

    // Same day number in previous month (or last day if shorter month)
    const lastDayOfPrevMonth = new Date(date.getFullYear(), date.getMonth(), 0).getDate();
    const prevMonthDay = Math.min(currentDay - 1, lastDayOfPrevMonth); // -1 for yesterday equivalent

    const prevMonthEnd = new Date(date.getFullYear(), date.getMonth() - 1, prevMonthDay);

    return {
      start: this.formatDateISO(prevMonthStart),
      end: this.formatDateISO(prevMonthEnd)
    };
  }

  static getSameMonthLastYearComplete(date) {
    const currentDay = date.getDate();

    // First day of same month last year
    const lastYearStart = new Date(date.getFullYear() - 1, date.getMonth(), 1);

    // Same day number last year (or last day if shorter month)
    const lastDayOfLastYearMonth = new Date(date.getFullYear() - 1, date.getMonth() + 1, 0).getDate();
    const lastYearDay = Math.min(currentDay - 1, lastDayOfLastYearMonth); // -1 for yesterday equivalent

    const lastYearEnd = new Date(date.getFullYear() - 1, date.getMonth(), lastYearDay);

    return {
      start: this.formatDateISO(lastYearStart),
      end: this.formatDateISO(lastYearEnd)
    };
  }

  static getThreeMonthAverageComplete(date) {
    const currentDay = date.getDate();

    // 3 months ago, first day
    const threeMonthsAgo = new Date(date.getFullYear(), date.getMonth() - 3, 1);

    // End of last month, same day number as yesterday
    const lastMonth = new Date(date.getFullYear(), date.getMonth() - 1, 1);
    const lastDayOfLastMonth = new Date(date.getFullYear(), date.getMonth(), 0).getDate();
    const endDay = Math.min(currentDay - 1, lastDayOfLastMonth);

    const threeMonthEnd = new Date(date.getFullYear(), date.getMonth() - 1, endDay);

    return {
      start: this.formatDateISO(threeMonthsAgo),
      end: this.formatDateISO(threeMonthEnd)
    };
  }

  static getCurrentMonth() {
    return this.formatMonthISO(new Date());
  }

  static getLastCompletedMonth() {
    const date = new Date();
    const lastMonth = new Date(date.getFullYear(), date.getMonth() - 1, 1);
    return this.formatMonthISO(lastMonth);
  }

  static getPrevious11Months() {
    const months = [];
    const now = new Date();

    for (let i = 2; i <= 12; i++) {
      const monthDate = new Date(now.getFullYear(), now.getMonth() - i, 1);
      months.push(this.formatMonthISO(monthDate));
    }

    return months.reverse();
  }

  static getPrevious12CompletedMonths() {
    const months = [];
    const now = new Date();

    for (let i = 1; i <= 12; i++) {
      const monthDate = new Date(now.getFullYear(), now.getMonth() - i, 1);
      months.push(this.formatMonthISO(monthDate));
    }

    return months.reverse();
  }

  static generateDateRange(startDate, endDate) {
    const dates = [];
    const start = new Date(startDate);
    const end = new Date(endDate);

    const current = new Date(start);
    while (current <= end) {
      dates.push(this.formatDateISO(current));
      current.setDate(current.getDate() + 1);
    }

    return dates;
  }

  static isMonthEnd(date = new Date()) {
    const testDate = new Date(date);
    const nextDay = new Date(testDate);
    nextDay.setDate(testDate.getDate() + 1);
    return testDate.getMonth() !== nextDay.getMonth();
  }

  static getYesterday() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    return this.formatDateISO(yesterday);
  }

  static getToday() {
    return this.formatDateISO(new Date());
  }
}

export default ChurnGuardCalendar;