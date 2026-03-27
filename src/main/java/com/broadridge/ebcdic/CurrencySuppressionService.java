package com.broadridge.ebcdic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service to handle currency-based output suppression based on 10100-ACH-PRINT-IND field.
 * When ACH-PRINT-IND is "0" for a specific account and currency, all output for that 
 * account+currency combination should be suppressed.
 */
public final class CurrencySuppressionService {
    private static final Logger logger = LogManager.getLogger(CurrencySuppressionService.class);
    private static final CurrencySuppressionService INSTANCE = new CurrencySuppressionService();

    // Map key format: "accountNumber|currency"
    private final Map<String, String> suppressionMap = new ConcurrentHashMap<>();
    private volatile boolean enabled;

    private CurrencySuppressionService() {
        enabled = false;
    }

    public static CurrencySuppressionService getInstance() {
        return INSTANCE;
    }

    /**
     * Enable or disable currency suppression feature
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        logger.info("Currency suppression service enabled={}", enabled);
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Register a suppression rule for account+currency combination
     * @param accountNumber The account number
     * @param currency The currency code (e.g., "000")
     * @param achPrintInd The ACH print indicator ("0" = suppress, "1" or other = allow)
     */
    public void registerSuppression(String accountNumber, String currency, String achPrintInd) {
        if (!enabled) {
            return;
        }

        if (accountNumber == null || currency == null || achPrintInd == null) {
            return;
        }

        String key = createKey(accountNumber.trim(), currency.trim());
        String value = achPrintInd.trim();
        
        suppressionMap.put(key, value);
        
        if ("0".equals(value)) {
            logger.debug("Registered currency suppression for account={}, currency={}", 
                        accountNumber.trim(), currency.trim());
        }
    }

    /**
     * Check if output should be emitted for given account+currency combination
     * @param accountNumber The account number
     * @param currency The currency code
     * @return true if output should be generated, false if suppressed
     */
    public boolean shouldEmit(String accountNumber, String currency) {
        if (!enabled) {
            return true;
        }

        if (accountNumber == null || currency == null) {
            return true;
        }

        String key = createKey(accountNumber.trim(), currency.trim());
        String achPrintInd = suppressionMap.get(key);
        
        // If no explicit rule found, default to emit (allow output)
        if (achPrintInd == null) {
            return true;
        }

        // Suppress only when ACH-PRINT-IND is "0"
        boolean shouldEmit = !"0".equals(achPrintInd);
        
        if (!shouldEmit) {
            logger.debug("Suppressing output for account={}, currency={} (ACH-PRINT-IND={})", 
                        accountNumber.trim(), currency.trim(), achPrintInd);
        }
        
        return shouldEmit;
    }

    /**
     * Clear all suppression rules (useful for testing or reset)
     */
    public void clear() {
        suppressionMap.clear();
        logger.info("Cleared all currency suppression rules");
    }

    /**
     * Get count of registered suppression rules
     */
    public int getSuppressionCount() {
        return suppressionMap.size();
    }

    /**
     * Create a unique key for account+currency combination
     */
    private String createKey(String accountNumber, String currency) {
        return accountNumber + "|" + currency;
    }

    /**
     * Get all registered suppression rules
     * @return Map of account|currency keys to ACH-PRINT-IND values
     */
    public Map<String, String> getSuppressedAccounts() {
        return new HashMap<>(suppressionMap);  // Return a copy
    }
}
