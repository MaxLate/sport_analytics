{% docs date_day %}

__date_day__ represents the calendar day to which a sleep session belongs.  
Because many sleep sessions start shortly after midnight, the raw sleep onset date
does not always match the logical “night” for analysis.

If a session begins between *00:00* and *11:59* **and** is not a nap, it is considered part of the **previous night**.  
All other sessions keep the sleep onset date as their night date.  
This ensures consistent attribution of sleep cycles to the correct night.

**SQL logic:**
```sql
case
    when extract(hour from sleep_onset) < 12
         and nap is false
        then (sleep_onset::date - interval '1 day')::date
    else sleep_onset::date
end

{% enddocs %}
