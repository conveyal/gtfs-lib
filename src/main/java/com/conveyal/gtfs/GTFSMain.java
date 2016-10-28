package com.conveyal.gtfs;

import com.conveyal.gtfs.stats.FeedStats;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipFile;

public class GTFSMain {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSMain.class);

    public static void main (String[] args) throws Exception {

        if (args.length < 1) {
            System.out.printf("specify a GTFS feed to load.");
        }
//        File tempFile = File.createTempFile("gtfs", ".db");
//
//        GTFSFeed feed = new GTFSFeed(tempFile.getAbsolutePath());
//        feed.loadFromFile(new ZipFile(args[0]));
//
//        feed.findPatterns();
//        feed.validate();
//        System.out.println(feed.errors.size());
//        feed.close();
//        String[] array = {"Atlantic_Express_20141112T194641-05_ef3693b8-24a4-4c81-9e47-56ceca77023f", "Access_Allegany_20141112T191853-05_7a10a8ac-d448-4fab-9779-9599c63d5c96", "Adirondack_Trailways_20141112T194554-05_8ed7fcce-bb9f-42bd-b1b7-5e006819e564", "Amtrak_NY_Only_20141205T115752-05_be5b775b-6811-4522-bbf6-1a408e7cf3f8", "Birnie_Bus_20141112T194815-05_24e99790-211d-4f92-b1d2-147e6f3d5040", "Bronx_Bus_20160404T110302-04_ca418447-35a5-4bce-8ced-d230616e7ff5", "Brooklyn_Bus_20160404T110310-04_c3ffe8d4-7331-4ac2-90e0-b59fd5c5b014", "Broome_County_Transit_20150313T172823-04_ad37dc59-12a3-4087-bb07-2e5ff4ad7d26", "C_TRAN_20151027T103335-04_033cd499-a215-4d13-b00d-24c9c4bf9cd0", "CARTS_20150609T174716-04_74eb8a63-c7bd-4ea0-8361-e58c87acd7d7", "CDTA_20160826T160227Z_8ff67add-9cd3-48f2-98c9-961dd6f331d7", "CEATS_20141112T202103-05_cd1c3566-0cac-44f6-95a9-968672b9b82a", "Centro_20160223T155210-05_63dcf8a5-b2c6-470e-849f-36b72022af16", "City_Of_Glen_Cove_20141231T113603-05_4787c168-6a14-4fe2-b3ba-c0eb7a093885", "City_of_Long_Beach_Bus_20141119T173251-05_3c9c2c79-851b-45cc-b042-bfb7746e4fe0", "Clarkstown_MiniTrans_20141112T203846-05_527b7a58-6488-4fd8-b4f2-3ee65cde6fc1", "CNY_Centro_Inc_A_Division_of_CNYRTA_-_Cayuga_County_20150219T130414-05_228e022b-b362-465b-879f-5926e76d52ee", "CNY_Centro_Inc_A_Division_of_CNYRTA_-_City_of_Rome_20150219T130451-05_9484a0ba-aa45-421f-b7cf-2b77fe81a84c", "CNY_Centro_Inc_A_Division_of_CNYRTA_-_City_of_Utica_20150219T130545-05_a5fa05c8-c356-4330-8912-88f8ff39209b", "CNY_Centro_Inc_A_Division_of_CNYRTA_-_Onondaga_County_20150219T144902-05_4da7e2eb-456c-4027-a3ec-1f04ff0a428c", "CNY_Centro_Inc_A_Division_of_CNYRTA_-_Oswego_County_20150219T130619-05_65867fea-c4d7-40a9-952d-192c20a8a67a", "Cross_Sound_Ferry_20141112T204155-05_96c53f2e-9494-4f08-9891-39f85286ad9b", "Dutchess_County_Transit_20150910T132853-04_706eac84-8c1a-475e-b31b-bd9247980062", "Essex_County_Public_Transportation_20160429T151707-04_ee3c2293-1923-4254-87e1-49a9643bfeae", "Fire_Island_Ferry_20141021T134323-04_442166a4-2265-40e3-b0fb-4795398c7462", "Franklin_County_Public_Transportation_20150312T155736-04_ac400211-bad0-4911-86e6-7ca49f15d96b", "Gloversville_Transit_Services_20141112T204502-05_b50befa0-cab1-4b8a-950f-93304e2661c1", "Greater_Glens_Falls_Transit_System_20141112T204733-05_c97061f2-f2d1-4133-8a71-30423f0dab7b", "HART_Huntington_Area_Rapid_Transit_20141112T204922-05_ad06e0ce-6ab2-435d-847d-401877bcf203", "Hornell_Area_Transit_20141112T205140-05_2a279c63-b22f-4851-9409-ceed8162bdcd", "IBUS_20141112T205259-05_af9abe7d-e63c-4096-8d81-bea7b6820324", "JFK_Airtrain_20160905T172918Z_ff8e7b01-c04e-4cb1-9615-32dc64179c5e", "Kingston_CitiBus_20141112T221521-05_3d09ebde-0383-47fe-906d-34aa19e90067", "Leprechaun_Lines_20141112T221814-05_0ec4aeb1-01d5-45cc-97a1-d424ade5f78b", "Liberty_Water_Taxi_20141112T222404-05_0c30fea3-c918-4925-bcbb-a78504cde6ea", "Long_Island_Rail_Road_20160826T160227Z_fb73d63b-29e1-4a76-be74-0c3d3c5dd341", "Madison_Transit_Services_20150316T140236-04_54d9faf2-a6cf-47b0-b08f-935328b82772", "Manhattan_Bus_20160404T110358-04_c27664d6-1e6a-443d-aa32-ca247777f7c8", "Medium_Village_Transit_20160913T145809Z_874846ab-c796-4f68-b48c-f6a26659d98c", "Metro-North_Railroad_20160826T160227Z_f3bfba4d-7137-49e7-8796-a53efc989458", "Middletown_Transit_Corp_20141112T222646-05_a4aa8e9e-e777-44b0-ba82-918bae582068", "MTA_Bus_Company_20160404T105848-04_c80ed7f6-e374-405a-89e8-dfe34bc734a6", "MTA_New_York_City_Subway_20160826T160227Z_5ab746a6-a22a-4d58-8567-611e03e718a8", "Nassau_Inter-County_Express_20160505T114308-04_1eef5b51-001f-41a4-9a71-47689a65477b", "Newark_Airtrain_20141021T133508-04_c9084cd3-1682-4426-ae42-49f50de74145", "Newburgh_Beacon_Bus_20141021T133509-04_4393e20e-3d9a-41c6-813f-a2baf86d9e0f", "NFTA_-_Metro_20160523T101544-04_e5bd2178-4ea9-4c91-977b-f200e80171e9", "NJ_TRANSIT_BUS_20160524T121737-04_ef2a1e8e-b33d-4e65-9bc2-93c1b691ea6c", "NJ_TRANSIT_Rail_20160523T102338-04_313bfca7-0436-4bcc-bb25-777b895dfc5f", "Norwalk_Transit_20141021T133538-04_d97be592-72f4-45b6-bb1f-0ab3a83aadab", "NY_Water_Taxi_20141021T142416-04_7cea9981-4c45-4e11-962a-cf648d9922c8", "NY_Waterway_20141021T133559-04_40695f32-635d-4793-892b-f7ed97d28d24", "Oneonta_Public_Transportation_20141021T133601-04_3631a150-1b0c-4445-b474-4445ba1ca2d9", "PART_Putnam_Area_Rapid_Transit_20150129T064504-05_9978c06b-dabc-4c37-8ff0-14cd7028f9b1", "PATH_20141021T133603-04_fca1b135-2a9f-44dc-a17e-27a52f4cf2ec", "Port_Authority_Trans-Hudson_Corporation_20160826T160227Z_a08cf935-3092-481e-af6c-b97a47565dc4", "Poughkeepsie_Transit_20150804T225828-04_0aa7999d-65fa-457e-b8ba-1b0d50e72bf1", "Queens_Bus_20160404T110423-04_225f831b-fa0e-4138-a62d-13048e9fd3a3", "Rochester-Genesee_Regional_Transportation_Authority__20160826T160227Z_5dfac884-9e86-44f1-a041-2fa1da57b3dc", "Rockland_County_Department_of_Public_Transportation_20141021T133655-04_8db7f031-73b6-4f26-8708-4b4a14f3d3ba", "Schoharie_County_Public_Transit_20141104T102500-05_aa469242-cfa3-4790-b59a-b91e227a6967", "Seastreak_20141021T133641-04_aa6cf70d-43d9-4567-aac3-9f74d2fe76ec", "St_Lawrence_County_Public_Transportation_20141114T102907-05_d369e596-8ed3-4b17-ba4b-b8c2b5a234c0", "Staten_Island_Bus_20160404T110453-04_3effd49a-3b7a-46bb-8453-950496682dc6", "Staten_Island_Ferry_20150828T171738-04_efbdb433-c984-4ce0-b5ef-ae4534b799d5", "Steuben_County_Transit_20141021T133653-04_94515905-4dbc-412f-92d8-678bd8f577c1", "Suffolk_Transit_20141208T110932-05_87c64274-bcf2-470f-99de-d14d065dff99", "TappanZee_Express_20160926T150726Z_927e6b67-3c1c-4252-ae12-d4a856689dc9", "TCAT_Tompkins_Consolidated_Area_Transit_20141021T133657-04_46d8975f-6706-4f76-aa9d-67368461529a", "Test_20160819T181205Z_aa30019b-fbc7-4f85-9ba5-8367a8f3e19e", "test_20160808T211651Z_cf95925c-3fef-487f-b566-3f5ff53d5419", "UCAT_Ulster_County_Area_Transit_20160426T095355-04_90029cc1-383a-414a-bd8d-44127e4cc1ff", "Warwick_Transit_20141104T103516-05_f83004cf-13e3-46c6-8f89-c1eccf48d402", "Watertown_Citibus_20141021T133701-04_86513906-cb98-44fb-9e18-a50ca11f7a67", "Westchester_County_Bee-Line_System_20160926T171434Z_e3571abf-0284-4b6d-8a3c-0a9f8dbd8d33"};
        GTFSCache cache = new GTFSCache("datatools-gtfs-mtc", "gtfs", new File("/Users/landon/conveyal/manager-scratch/gtfs/"));
        GTFSFeed feed;
        try {
            feed = cache.get("ASC-20161024T142439-04-81f58491-32fb-4e8c-b09e-1c0d98b32129");
        } catch (Exception e) {
            LOG.error("Could not load feed", e);
        }
//        for (String id : array) {
//            GTFSFeed feed;
//            try {
//                feed = cache.get(id);
//                feed.validate();
//                FeedStats stats = new FeedStats(feed);
//            System.out.println("Num dates: " + feed.getDatesOfService().size());
//            System.out.println("Total revenue hours: " + stats.getTotalRevenueTime() / 60 / 60);
//            System.out.println("Avg weekday hours: " + stats.getAverageWeekdayRevenueTime() / 60 / 60);
//                System.out.println("Avg Tuesday hours: " + stats.getAverageDailyRevenueTime(2) / 60 / 60);
//                System.out.println("ID: " + id);
//            } catch (Exception e) {
//                LOG.info("Could not open {}", id);
//            }
//        }
//        GTFSFeed reconnected = cache.get("CobbLinc-20161018T084451-04-e50738df-ce3f-4e1d-9ab6-7aba656b31bd");
        LOG.info("reopening feed");

        // re-open
//        GTFSFeed reconnected = new GTFSFeed(tempFile.getAbsolutePath());
//
//        LOG.info("Connected to already loaded feed");
//
//        LOG.info("  {} routes", reconnected.routes.size());
//        LOG.info("  {} trips", reconnected.trips.size());
//        LOG.info("  {} stop times", reconnected.stop_times.size());
//        LOG.info("  Feed ID: {}", reconnected.feedId);
    }

}
