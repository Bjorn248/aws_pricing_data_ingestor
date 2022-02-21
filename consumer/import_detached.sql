IMPORT INTO AmazonEC2 (SKU, OfferTermCode, RateCode, TermType, PriceDescription, EffectiveDate, StartingRange, EndingRange, Unit, PricePerUnit, Currency, RelatedTo, LeaseContractLength, PurchaseOption, OfferingClass, ProductFamily, ServiceCode, Location, LocationType, InstanceType, CurrentGeneration, InstanceFamily, vCPU, PhysicalProcessor, ClockSpeed, Memory, Storage, NetworkPerformance, ProcessorArchitecture, StorageMedia, VolumeType, MaxVolumeSize, MaxIOPSVolume, MaxIOPSBurstPerformance, MaxThroughputPerVolume, Provisioned, Tenancy, EBSOptimized, OS, LicenseModel, AWSGroup, AWSGroupDescription, TransferType, FromLocation, FromLocationType, ToLocation, ToLocationType, UsageType, Operation, AvailabilityZone, CapacityStatus, ClassicNetworkingSupport, DedicatedEBSThroughput, ECU, Elastic_Graphics_Type, EnhancedNetworkingSupported, From_Region_Code, GPU, GPU_Memory, Instance, InstanceCapacity10xLarge, Instance_Capacity__12xlarge, Instance_Capacity__16xlarge, Instance_Capacity__18xlarge, Instance_Capacity__24xlarge, InstanceCapacity2xLarge, Instance_Capacity__32xlarge, InstanceCapacity4xLarge, InstanceCapacity8xLarge, Instance_Capacity__9xlarge, InstanceCapacityLarge, InstanceCapacityMedium, Instance_Capacity__metal, InstanceCapacityxLarge, instanceSKU, IntelAVXAvailable, IntelAVX2Available, IntelTurboAvailable, MarketOption, NormSizeFactor, PhysicalCores, PreInstalledSW, ProcessorFeatures, Product_Type, Region_Code, Resource_Type, serviceName, SnapshotArchiveFeeType, To_Region_Code, Volume_API_Name, VPCNetworkingSupport)
     DELIMITED DATA (
       'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.csv'
     )
    WITH
      fields_terminated_by=',',
      fields_enclosed_by='"',
      skip='6',
      DETACHED;

