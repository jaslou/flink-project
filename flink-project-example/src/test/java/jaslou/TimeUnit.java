/*    */ package jaslou;
/*    */ 
/*    */ import java.math.BigDecimal;
/*    */ 
/*    */ public enum TimeUnit
/*    */ {
/* 25 */   YEAR(true, ' ', 12L, null), 
/* 26 */   MONTH(true, '-', 1L, BigDecimal.valueOf(12L)), 
/* 27 */   DAY(false, '-', 86400000L, null), 
/* 28 */   HOUR(false, ' ', 3600000L, BigDecimal.valueOf(24L)), 
/* 29 */   MINUTE(false, ':', 60000L, BigDecimal.valueOf(60L)), 
/* 30 */   SECOND(false, ':', 1000L, BigDecimal.valueOf(60L));
/*    */ 
/*    */   public final boolean yearMonth;
/*    */   public final char separator;
/*    */   public final long multiplier;
/*    */   private final BigDecimal limit;
/* 37 */   private static final TimeUnit[] CACHED_VALUES = values();
/*    */ 
/*    */   private TimeUnit(boolean yearMonth, char separator, long multiplier, BigDecimal limit)
/*    */   {
/* 41 */     this.yearMonth = yearMonth;
/* 42 */     this.separator = separator;
/* 43 */     this.multiplier = multiplier;
/* 44 */     this.limit = limit;
/*    */   }
/*    */ 
/*    */   public static TimeUnit getValue(int ordinal)
/*    */   {
/* 52 */     return (ordinal < 0) || (ordinal >= CACHED_VALUES.length) ? null : CACHED_VALUES[ordinal];
/*    */   }
/*    */ 
/*    */   public boolean isValidValue(BigDecimal field)
/*    */   {
/* 64 */     return (field.compareTo(BigDecimal.ZERO) >= 0) && ((this.limit == null) || (field.compareTo(this.limit) < 0));
/*    */   }
/*    */ }

/* Location:           C:\Users\jaslou\Downloads\主会场\calcite-avatica-1.5.0.jar
 * Qualified Name:     org.apache.calcite.avatica.util.TimeUnit
 * JD-Core Version:    0.6.2
 */