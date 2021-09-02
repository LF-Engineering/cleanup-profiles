# cleanup-profiles
Cleanup profiles

Usage:
- `[DELETE_ORPHANED=1] [N_CPUS=12] [DEBUG=1] [SQLDEBUG=1] [DRY=1] CLEANUP_PROFILES=1 ./cleanup.sh test|prod 2>&1 | tee run.log`.


# cleanup identities incorrect emails

Usage:
- `[SKIP_VALIDATE_DOMAIN=1] [SKIP_GUESS_EMAIL=1] [SKIP_IDENTITIES=1] [SKIP_PROFILES=1] [N_CPUS=12] [DEBUG=1] [SQLDEBUG=1] [DRY=1] CLEANUP_EMAILS=1 ./cleanup.sh test|prod 2>&1 | tee run.log`.


# validate emails

Usage:
- `[SKIP_VALIDATE_DOMAIN=1] [SKIP_GUESS_EMAIL=1] CHECK_EMAILS='address@domain.com,adr2@abc.com.pl' ./cleanup.sh test`



