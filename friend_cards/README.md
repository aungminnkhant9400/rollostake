# Friend Prediction Cards

Store the friend's weekly prediction HTML files here.

These files are studied as aggregate structure only. RolloStake should learn market mix, odds bands, protected-side usage, team-total usage, and risk-band shape from them, but it must not blindly copy the exact picks.

Current workflow:

```powershell
python scripts\study_external_card.py friend_cards
```

When a new card arrives, copy it into this folder, rerun the command above, then rebuild the dashboard.
