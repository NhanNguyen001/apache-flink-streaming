package com.nhannt22.utils;

import com.nhannt22.metadata.InternalAccountDto;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.List;

import static com.nhannt22.utils.DateUtils.toVietNameTimeZone;

public class DataUtils {

    private static final int effectiveHour = 11;
    private static final int effectiveMinute = 0;
    private static final int effectiveSecond = 0;

    public static List<InternalAccountDto> getInternalAccount(){
        return FileUtils.readFileInternalAccount();
    }

    public static  DataStreamValid getDataStreamValid(String accountId, String phase, String accountAddress) {
        /**
         * Ignore internal account
         */
        boolean isInternalAccount = getInternalAccount()
                .stream()
                .filter(account -> account.getAcctId().equals(accountId))
                .map(account -> true)
                .findFirst().orElse(false);

        /**
         * Posting phase valid
         */
        boolean isPostingValid = phase.equalsIgnoreCase("POSTING_PHASE_COMMITTED");

        /**
         * Account address valid
         */
        boolean isAccountAddressValid = accountAddress.equalsIgnoreCase("DEFAULT");

        return new DataStreamValid(isInternalAccount, isPostingValid, isAccountAddressValid);
    }

    public static class DataStreamValid {
        public final boolean isInternalAccount;
        public final boolean isPostingValid;
        public final boolean isAccountAddressValid;

        public DataStreamValid(boolean isInternalAccount, boolean isPostingValid, boolean isAccountAddressValid) {
            this.isInternalAccount = isInternalAccount;
            this.isPostingValid = isPostingValid;
            this.isAccountAddressValid = isAccountAddressValid;
        }

        public boolean isInternalAccount() {
            return isInternalAccount;
        }

        public boolean isPostingValid() {
            return isPostingValid;
        }
        public boolean isAccountAddressValid() {
            return isAccountAddressValid;
        }
    }

    public static String getTenor(Integer tenor, String unit){
        if (tenor == null) {
            return null;
        }

        if (StringUtils.isNotEmpty(unit) && unit.equals("week")) {
            tenor = convertToMonth(tenor);
        }

        if (tenor < 12) {
            return "NGANHAN";
        } else if (tenor >= 12 && tenor < 36) {
            return "TRUNGHAN";
        } else if (tenor >= 36 && tenor < 60) {
            return "DAIHAN";
        }

        return null;
    }

    public static String convertPeriodType(String periodType){
        if (StringUtils.isNotEmpty(periodType)) {
            if (periodType.equals("week")) {
                return  "W";
            }else {
                return "M";
            }
        }
        return null;
    }
    public static boolean isDataValid(DataUtils.DataStreamValid streamData) {
        return streamData.isPostingValid() && !streamData.isInternalAccount() && streamData.isAccountAddressValid();
    }

    public static int convertToMonth(int number) {
        return number / 4;
    }

    public static String getCif(List<String> cifs){
        return String.join(",", cifs);
    }


    public static LocalDateTime calculateEffectiveDateTime(String depositStartDateTimeStr) {
        LocalDateTime depositStartDatetime = DateUtils.convertToLocalDateTime(depositStartDateTimeStr);

        if (depositStartDatetime == null) {
            return null;
        }

        LocalTime effectiveTime =
                LocalTime.of(
                        effectiveHour, effectiveMinute , effectiveSecond);
        LocalDateTime effectiveDateTime =
                depositStartDatetime.toLocalTime().isBefore(effectiveTime)
                        ? depositStartDatetime
                        : depositStartDatetime.plusDays(1);
        return effectiveDateTime
                .withHour(effectiveHour)
                .withMinute(effectiveMinute)
                .withSecond(effectiveSecond);
    }

    public static LocalDate calculateMaturityDateByTenor(
            LocalDateTime effectiveDateTime, String tenorUnit, Integer tenorAmount) {
        int timeGap = 18;

        if (effectiveDateTime == null) {
            return null;
        }

        LocalDateTime maturityDateTime;

        switch (tenorUnit.toUpperCase()) {
            case "MONTH":
                maturityDateTime = DateUtils.plusMonth(effectiveDateTime, tenorAmount);
                break;
            case "WEEK":
                maturityDateTime = DateUtils.plusWeek(effectiveDateTime, tenorAmount);
                break;
            default:
                return null;
        };

        if ("MONTH".equalsIgnoreCase(tenorUnit)) {
            int dayInMonthOfOpening = YearMonth.from(effectiveDateTime).atEndOfMonth().getDayOfMonth();

            if (dayInMonthOfOpening == effectiveDateTime.getDayOfMonth()) {
                int dayInMonthOfMaturity = YearMonth.from(maturityDateTime).atEndOfMonth().getDayOfMonth();
                maturityDateTime = maturityDateTime.withDayOfMonth(dayInMonthOfMaturity);
            }
        }

        return maturityDateTime.minusHours(timeGap).toLocalDate();
    }
}
