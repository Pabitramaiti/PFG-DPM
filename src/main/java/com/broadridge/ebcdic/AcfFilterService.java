package com.broadridge.ebcdic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ACF (Account Complement Filter) service for B228 processing.
 *
 * Applies include/exclude logic based on a filter config file (HDR/DTL/TLR format)
 * keyed on FIRM, AltBranch, REP and AccountNumber.
 *
 * FIRM is resolved by looking up the AltBranch in the branch file
 * (positions 1-4 = firmId, positions 252-256 = altBranch, both 1-indexed).
 *
 * This service is a singleton and is enabled/disabled via {@link #setEnabled(boolean)}.
 * When disabled it always returns {@code true} (emit all records).
 *
 * Rules are evaluated independent of file order.
 *
 * Precedence:
 * 1) More specific matching rules win over less specific rules.
 * 2) If two matching rules have equal specificity, exclusion (E) wins over include (I).
 */
public final class AcfFilterService {

    private static final Logger logger = LogManager.getLogger(AcfFilterService.class);
    private static final AcfFilterService INSTANCE = new AcfFilterService();

    /** altBranch → firmId, built from the branch file. */
    private final Map<String, String> altBranchToFirmId = new HashMap<>();

    /** List of rules read from the ACF config file. */
    private final List<AcfRule> rules = new ArrayList<>();

    private volatile boolean enabled = false;

    private AcfFilterService() {}

    public static AcfFilterService getInstance() {
        return INSTANCE;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        logger.info("AcfFilterService enabled={}", enabled);
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** Reset all state (rules + branch map). */
    public void clear() {
        altBranchToFirmId.clear();
        rules.clear();
        enabled = false;
    }

    // -------------------------------------------------------------------------
    // Loading
    // -------------------------------------------------------------------------

    /**
     * Load the branch file to build the altBranch → firmId lookup map.
     * <ul>
     *   <li>Firm number : positions 1-4  (0-indexed: 0–3)</li>
     *   <li>Alt Branch  : positions 252-256 (0-indexed: 251–255)</li>
     * </ul>
     * Header lines starting with "HDR" are skipped, as are lines shorter than 256 chars.
     */
    public void loadBranchFile(String branchFilePath) {
        altBranchToFirmId.clear();
        if (branchFilePath == null || branchFilePath.isBlank()) {
            logger.info("No branch file provided; ACF firm-lookup map will be empty.");
            return;
        }
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(branchFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("HDR")) continue;
                if (line.length() < 256) continue;
                String firmId    = line.substring(0, 4).trim();
                String altBranch = line.substring(251, 256).trim();
                if (!firmId.isEmpty() && !altBranch.isEmpty()) {
                    altBranchToFirmId.put(altBranch, firmId);
                    count++;
                }
            }
            logger.info("Loaded {} branch→firm mappings from {}", count, branchFilePath);
        } catch (IOException e) {
            logger.error("Unable to read branch file {}", branchFilePath, e);
        }
    }

    /**
     * Load the ACF filter config file.
     * <pre>
     * HDR,YYYYMMDD
     * DTL,I|E,FIRM,AltBranch,REP,AccountNumber
     * TLR,NNNNNNNNNN
     * </pre>
    * Any empty trailing fields are treated as wildcards (match-all for that dimension).
     */
    public void loadAcfFile(String acfFilePath) {
        rules.clear();
        if (acfFilePath == null || acfFilePath.isBlank()) {
            logger.info("No ACF filter file provided.");
            return;
        }
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(acfFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                if (line.startsWith("HDR") || line.startsWith("TLR") || line.startsWith("TRL")) continue;
                if (!line.startsWith("DTL")) continue;

                String[] parts = line.split(",", -1);
                if (parts.length < 3) {
                    logger.warn("Skipping ACF line (too few fields): {}", line);
                    continue;
                }

                String indicator = safe(parts, 1);
                String firm      = safe(parts, 2);
                String altBranch = safe(parts, 3);
                String rep       = safe(parts, 4);
                String account   = safe(parts, 5);

                if (!"I".equalsIgnoreCase(indicator) && !"E".equalsIgnoreCase(indicator)) {
                    logger.warn("Skipping ACF line with unknown indicator '{}': {}", indicator, line);
                    continue;
                }
                if (firm.isEmpty()) {
                    logger.warn("Skipping ACF line with empty FIRM: {}", line);
                    continue;
                }

                rules.add(new AcfRule(indicator, firm, altBranch, rep, account));
                count++;
            }
            logger.info("Loaded {} ACF filter rules from {}", count, acfFilePath);
        } catch (IOException e) {
            logger.error("Unable to read ACF filter file {}", acfFilePath, e);
        }
    }

    // -------------------------------------------------------------------------
    // Runtime
    // -------------------------------------------------------------------------

    /**
     * Return the firmId that is mapped to the given altBranch in the branch file,
     * or an empty string if no mapping exists.
     */
    public String lookupFirmId(String altBranch) {
        if (altBranch == null || altBranch.isEmpty()) return "";
        return altBranchToFirmId.getOrDefault(altBranch.trim(), "");
    }

    /**
     * Decide whether a B228 account statement should be emitted (included).
     *
    * <p>Rules are evaluated independent of file order.
    * All matching rules are considered; the most specific rule wins.
    * Specificity is the number of non-empty dimensions among
    * FIRM, AltBranch, REP and Account.
    * If multiple matching rules have equal specificity, exclusion wins.
     * Empty fields in a rule act as wildcards.
     * If no rule matches the default is <strong>suppress</strong> (return false).</p>
     *
     * <p>When the service is disabled ({@link #isEnabled()} == false) this method
     * always returns {@code true} so existing behaviour is unchanged.</p>
     *
     * @param firmId      firm ID resolved via branch-file lookup (from altBranch)
     * @param altBranch   20100-NAP-ALT-BRANCH value from the COMBO record
     * @param rep         20100-NAP-RR-NUMBER value from the COMBO record
     * @param account     full account number (XBASE-HDR-BR + XBASE-HDR-ACCT)
     * @return {@code true} to emit, {@code false} to suppress
     */
    public boolean shouldEmit(String firmId, String altBranch, String rep, String account) {
        if (!enabled) return true;

        String firm  = firmId   == null ? "" : firmId.trim();
        String alt   = altBranch == null ? "" : altBranch.trim();
        String repId = rep      == null ? "" : rep.trim();
        String acct  = account  == null ? "" : account.trim();

        AcfRule bestRule = null;
        int bestScore = -1;

        for (AcfRule rule : rules) {
            // FIRM must always match (never empty in a valid rule)
            if (!rule.firm.equalsIgnoreCase(firm)) continue;
            // Empty rule field = wildcard; non-empty must equal the record's value
            if (!rule.altBranch.isEmpty() && !rule.altBranch.equalsIgnoreCase(alt)) continue;
            if (!rule.rep.isEmpty()       && !rule.rep.equalsIgnoreCase(repId))      continue;
            if (!rule.account.isEmpty()   && !rule.account.equalsIgnoreCase(acct))    continue;

            int score = rule.specificityScore();
            if (score > bestScore) {
                bestScore = score;
                bestRule = rule;
            } else if (score == bestScore && bestRule != null) {
                // If equally specific, exclusion takes precedence.
                if (!rule.isInclude() && bestRule.isInclude()) {
                    bestRule = rule;
                }
            }
        }

        if (bestRule != null) {
            boolean emit = bestRule.isInclude();
            logger.debug("ACF filter selected rule [{},{},{},{},{}] for firm={} alt={} rep={} acct={} score={} → emit={}",
                    bestRule.indicator, bestRule.firm, bestRule.altBranch, bestRule.rep, bestRule.account,
                    firm, alt, repId, acct, bestScore, emit);
            return emit;
        }

        logger.debug("ACF filter: no rule matched for firm={} alt={} rep={} acct={} → suppress", firm, alt, repId, acct);
        return false;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String safe(String[] parts, int idx) {
        if (idx >= parts.length || parts[idx] == null) return "";
        return parts[idx].trim();
    }

    private static final class AcfRule {
        final String indicator;  // "I" or "E"
        final String firm;
        final String altBranch;  // empty = wildcard
        final String rep;        // empty = wildcard
        final String account;    // empty = wildcard

        AcfRule(String indicator, String firm, String altBranch, String rep, String account) {
            this.indicator = indicator == null ? "" : indicator.trim().toUpperCase();
            this.firm      = firm      == null ? "" : firm.trim();
            this.altBranch = altBranch == null ? "" : altBranch.trim();
            this.rep       = rep       == null ? "" : rep.trim();
            this.account   = account   == null ? "" : account.trim();
        }

        boolean isInclude() {
            return "I".equals(indicator);
        }

        int specificityScore() {
            int score = 1; // firm is always required in valid rules
            if (!altBranch.isEmpty()) score++;
            if (!rep.isEmpty()) score++;
            if (!account.isEmpty()) score++;
            return score;
        }
    }
}
