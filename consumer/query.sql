SELECT PricePerUnit
FROM   AmazonEC2
WHERE  TermType = 'OnDemand'
       AND Location = 'US East (N. Virginia)'
       AND InstanceType = 'm3.medium'
       AND Tenancy = 'Shared'
       AND OS = 'Linux'
       AND PreInstalledSW = 'NA'
       AND CapacityStatus = 'Used';
