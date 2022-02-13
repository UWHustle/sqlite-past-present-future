/* 
 *  Sccsid:     @(#)bcd2.h	2.1.8.1
 */

#include "config.h"

int bin_bcd2(DSS_HUGE binary, DSS_HUGE *low_res, DSS_HUGE *high_res);
int bcd2_bin(DSS_HUGE *dest, DSS_HUGE bcd);
DSS_HUGE bcd2_add(DSS_HUGE *bcd_low, DSS_HUGE *bcd_high, DSS_HUGE addend);
DSS_HUGE bcd2_sub(DSS_HUGE *bcd_low, DSS_HUGE *bcd_high, DSS_HUGE subend);
DSS_HUGE bcd2_mul(DSS_HUGE *bcd_low, DSS_HUGE *bcd_high, DSS_HUGE multiplier);
DSS_HUGE bcd2_div(DSS_HUGE *bcd_low, DSS_HUGE *bcd_high, DSS_HUGE divisor);
DSS_HUGE bcd2_mod(DSS_HUGE *bcd_low, DSS_HUGE *bcd_high, DSS_HUGE modulo);
DSS_HUGE bcd2_cmp(DSS_HUGE *bcd_low, DSS_HUGE *bcd_high, DSS_HUGE compare);
