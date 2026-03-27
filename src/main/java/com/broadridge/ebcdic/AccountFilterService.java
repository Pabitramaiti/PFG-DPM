//This module is used for account filtering .It handles both print and suppression rules

package com.broadridge.ebcdic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class AccountFilterService {
    private static final Logger logger = LogManager.getLogger(AccountFilterService.class);
    private static final AccountFilterService INSTANCE = new AccountFilterService();

    private final List<AccountFilterRule> rules = new CopyOnWriteArrayList<>();
    private volatile boolean enabled;

    private AccountFilterService() {}

    public static AccountFilterService getInstance() {
        return INSTANCE;
    }

    public void loadFromFile(String accountFile) {
        rules.clear();
        enabled = false;

        if (accountFile == null || accountFile.isEmpty()) {
            logger.info("No account filter file provided.");
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(accountFile))) {
            boolean header = true;
            String line;
            int count = 0;

            while ((line = reader.readLine()) != null) {
                if (header) {
                    header = false;
                    continue;
                }
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");
                if (parts.length != 3) {
                    logger.warn("Invalid filter row: {}", line);
                    continue;
                }

                String clientId = parts[0].trim();
                String accountRange = parts[1].trim();
                String printIndicator = parts[2].trim();

                if (accountRange.contains("-")) {
                    String[] range = accountRange.split("-");
                    if (range.length == 2) {
                        rules.add(new AccountFilterRule(clientId, range[0].trim(), range[1].trim(), printIndicator));
                        count++;
                    }
                } else {
                    rules.add(new AccountFilterRule(clientId, accountRange, printIndicator));
                    count++;
                }
            }

            enabled = count > 0;
            logger.info("Loaded {} account filter rules (enabled={})", count, enabled);
        } catch (IOException e) {
            logger.error("Unable to read account filter file {}", accountFile, e);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean shouldEmit(String accountNumber, String clientId) {
        if (!enabled) return true;
        return "P".equalsIgnoreCase(resolveIndicator(accountNumber, clientId));
    }

    private String resolveIndicator(String accountNumber, String clientId) {
        for (AccountFilterRule rule : rules) {
            if (!rule.clientId.equals(clientId)) continue;
            if (rule.matches(accountNumber)) return rule.printIndicator;
        }
        return "S";
    }

    private static final class AccountFilterRule {
        private final String clientId;
        private final String startAccount;
        private final String endAccount;
        private final boolean isSingle;
        private final String printIndicator;
        private final boolean isPrefix;

        AccountFilterRule(String clientId, String account, String printIndicator) {
            this.clientId = clientId;
            this.startAccount = account;
            this.endAccount = account;
            this.isSingle = true;
            this.printIndicator = printIndicator;
            this.isPrefix = account.length() < 4;
        }

        AccountFilterRule(String clientId, String start, String end, String printIndicator) {
            this.clientId = clientId;
            this.startAccount = start;
            this.endAccount = end;
            this.isSingle = false;
            this.printIndicator = printIndicator;
            this.isPrefix = start.length() < 4 || end.length() < 4;
        }

        boolean matches(String accountNumber) {
            if (isSingle) {
                return isPrefix ? accountNumber.startsWith(startAccount) : accountNumber.equals(startAccount);
            }
            return isPrefix ? inPrefixRange(accountNumber) : inRange(accountNumber);
        }

        private boolean inRange(String accountNumber) {
            if (accountNumber.length() != startAccount.length()) return false;
            return accountNumber.compareTo(startAccount) >= 0 && accountNumber.compareTo(endAccount) <= 0;
        }

        private boolean inPrefixRange(String accountNumber) {
            int len = Math.min(startAccount.length(), endAccount.length());
            if (accountNumber.length() < len) return false;
            String prefix = accountNumber.substring(0, len);
            return prefix.compareTo(startAccount) >= 0 && prefix.compareTo(endAccount) <= 0;
        }
    }
}