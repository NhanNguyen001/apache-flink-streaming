package com.nhannt22.learning;

public class CreditRecord {

    public String id;
    public String loanStatus;
    public String loanAmount;
    public String term;
    public String creditScore;
    public String annualIncome;
    public String home;
    public String creditBalance;

    public CreditRecord() {
    }

    public CreditRecord(String id, String loanStatus, String loanAmount,
                        String term, String creditScore, String annualIncome,
                        String home, String creditBalance) {
        this.id = id;
        this.loanStatus = loanStatus;
        this.loanAmount = loanAmount;
        this.term = term;
        this.creditScore = creditScore;
        this.annualIncome = annualIncome;
        this.home = home;
        this.creditBalance = creditBalance;
    }
}
