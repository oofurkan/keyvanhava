using System;
using System.Data.SqlClient;
using System.Configuration;
using System.IO;

namespace AirportDataConvertConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // Log dosyası yolu
                string logFilePath = "log.txt";

                // Log dosyasını oluştur
                StreamWriter logFile = File.AppendText(logFilePath);

                // Loglama başlığını yaz
                logFile.WriteLine($"{DateTime.Now} - Uygulama başlatıldı.");

                // Veritabanı bağlantı dizesi
                string connectionString = ConfigurationManager.ConnectionStrings["MyConnectionString"].ConnectionString;

                // Veritabanı bağlantısı oluştur
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    // Bağlantıyı aç
                    connection.Open();
                    Console.WriteLine("Veritabanına bağlanıldı.");
                    logFile.WriteLine($"{DateTime.Now} - Veritabanına bağlanıldı.");

                    // Veri okuma işlemi için komut oluştur (Version 22'deki tablo)
                    string queryV22 = "SELECT * FROM [dbo].[RUNWAY_V22]";
                    SqlCommand commandV22 = new SqlCommand(queryV22, connection);

                    // Veri okuma işlemini gerçekleştir
                    SqlDataReader readerV22 = commandV22.ExecuteReader();
                    Console.WriteLine("Version 22 tablosundan veriler okundu.");
                    logFile.WriteLine($"{DateTime.Now} - Version 22 tablosundan veriler okundu.");

                    // Version 22'deki verileri dönüştürerek ve version 19'daki tabloya ekleyerek aktarma
                    int rowCount = 0;
                    while (readerV22.Read())
                    {
                        rowCount++;

                        // Verileri al ve dönüştür
                        string runwayIdentifier = readerV22["RunwayIdentifier"].ToString();
                        string thresholdCrossingHeightRaw = readerV22["ThresholdCrossingHeight"].ToString();
                        string thresholdCrossingHeight = string.IsNullOrEmpty(thresholdCrossingHeightRaw) ? "00" : thresholdCrossingHeightRaw.TrimStart('0');
                        string runwayAccuracyComplianceFlag = readerV22["RunwayAccuracyComplianceFlag"].ToString();
                        string landingThresholdElevationAccuraryComplianceFlag = readerV22["LandingThresholdElevationAccuraryComplianceFlag"].ToString();

                        //etkilenmeyen verileri al 
                        string recordType = readerV22["RecordType"].ToString();
                        string customerAreaCode = readerV22["CustomerAreaCode"].ToString();
                        string sectionCode = readerV22["SectionCode"].ToString();
                        string airportICAOIdentifier = readerV22["AirportICAOIdentifier"].ToString();
                        string icaoCode = readerV22["IcaoCode"].ToString();
                        string subSectionCode = readerV22["SubSectionCode"].ToString();
                        string continuationNumber = readerV22["ContinuationNumber"].ToString();
                        string runwayLength = readerV22["RunwayLength"].ToString();
                        string runwayMagneticBearing = readerV22["RunwayMagneticBearing"].ToString();
                        string runwayLatitude = readerV22["RunwayLatitude"].ToString();
                        string runwayLongitude = readerV22["RunwayLongitude"].ToString();
                        string runwayGradient = readerV22["RunwayGradient"].ToString();
                        string ltpEllipsoidHeight = readerV22["LtpEllipsoidHeight"].ToString();
                        string landingThresholdElevation = readerV22["LandingThresholdElevation"].ToString();
                        string displacedThresholdDistance = readerV22["DisplacedThresholdDistance"].ToString();
                        string runwayWidth = readerV22["RunwayWidth"].ToString();
                        string tchValueIndicator = readerV22["TchValueIndicator"].ToString();
                        string stopway = readerV22["Stopway"].ToString();
                        string runwayDescription = readerV22["RunwayDescription"].ToString();
                        string cycle = readerV22["Cycle"].ToString();

                        // Veri dönüşümleri (örneğin, Threshold Crossing Height formatı)
                        // İlgili dönüşümleri gerçekleştirin

                        // Version 19'a ekleme işlemi
                        //bool success = AddToRunwayV19Table(runwayIdentifier, thresholdCrossingHeight, runwayAccuracyComplianceFlag, landingThresholdElevationAccuraryComplianceFlag);
                        bool success = AddToRunwayV19Table(runwayIdentifier, thresholdCrossingHeight, runwayAccuracyComplianceFlag, landingThresholdElevationAccuraryComplianceFlag, recordType, customerAreaCode, sectionCode, airportICAOIdentifier, icaoCode, subSectionCode, continuationNumber, runwayLength, runwayMagneticBearing, runwayLatitude, runwayLongitude, runwayGradient, ltpEllipsoidHeight, landingThresholdElevation, displacedThresholdDistance, runwayWidth, tchValueIndicator, stopway, runwayDescription, cycle);

                        if (success)
                        {
                            Console.WriteLine($"Kayıt {rowCount}: Version 19 tablosuna eklendi.");
                            logFile.WriteLine($"{DateTime.Now} - Kayıt {rowCount}: Version 19 tablosuna eklendi.");
                        }
                        else
                        {
                            Console.WriteLine($"Kayıt {rowCount}: Version 19 tablosuna eklenemedi.");
                            logFile.WriteLine($"{DateTime.Now} - Kayıt {rowCount}: Version 19 tablosuna eklenemedi.");
                        }
                    }

                    // Version 22 okuyucuyu kapat
                    readerV22.Close();
                }

                // Log dosyasını kapat
                logFile.Close();
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"SQL Hatası: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Genel Hata: {ex.Message}");
            }

        }

        // Version 19 tablosuna veri ekleme işlemi
        static bool AddToRunwayV19Table(string runwayIdentifier, string thresholdCrossingHeight, string runwayAccuracyComplianceFlag, string landingThresholdElevationAccuraryComplianceFlag, string recordType, string customerAreaCode, string sectionCode, string airportICAOIdentifier, string icaoCode, string subSectionCode, string continuationNumber, string runwayLength, string runwayMagneticBearing, string runwayLatitude, string runwayLongitude, string runwayGradient, string ltpEllipsoidHeight, string landingThresholdElevation, string displacedThresholdDistance, string runwayWidth, string tchValueIndicator, string stopway, string runwayDescription, string cycle)
        {
            try
            {
                // Veritabanı bağlantı dizesi
                string connectionString = ConfigurationManager.ConnectionStrings["MyConnectionString"].ConnectionString;

                // Veritabanı bağlantısı oluştur
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    // Bağlantıyı aç
                    connection.Open();

                    // Version 19 tablosuna ekleme işlemi için komut oluştur
                    string query = "INSERT INTO [dbo].[RUNWAY_V19] (RecordType, CustomerAreaCode, SectionCode, AirportICAOIdentifier, IcaoCode, SubSectionCode, RunwayIdentifier, ContinuationNumber, RunwayLength, RunwayMagneticBearing, RunwayLatitude, RunwayLongitude, RunwayGradient, LtpEllipsoidHeight, LandingThresholdElevation, DisplacedThresholdDistance,ThresholdCrossingHeight, RunwayWidth, TchValueIndicator, Stopway, RunwayDescription, Cycle) " +
               "VALUES " +
               "(@RecordType, @CustomerAreaCode, @SectionCode, @AirportICAOIdentifier, @IcaoCode, @SubSectionCode, @RunwayIdentifier, @ContinuationNumber, @RunwayLength, @RunwayMagneticBearing, @RunwayLatitude, @RunwayLongitude, @RunwayGradient, @LtpEllipsoidHeight, @LandingThresholdElevation, @DisplacedThresholdDistance,@ThresholdCrossingHeight, @RunwayWidth, @TchValueIndicator, @Stopway, @RunwayDescription, @Cycle)";
                    SqlCommand command = new SqlCommand(query, connection);

                    // Parametrelerin atanması
                    command.Parameters.AddWithValue("@RecordType", recordType);
                    command.Parameters.AddWithValue("@CustomerAreaCode", customerAreaCode);
                    command.Parameters.AddWithValue("@SectionCode", sectionCode);
                    command.Parameters.AddWithValue("@AirportICAOIdentifier", airportICAOIdentifier);
                    command.Parameters.AddWithValue("@IcaoCode", icaoCode);
                    command.Parameters.AddWithValue("@SubSectionCode", subSectionCode);
                    command.Parameters.AddWithValue("@RunwayIdentifier", runwayIdentifier);
                    command.Parameters.AddWithValue("@ContinuationNumber", continuationNumber);
                    command.Parameters.AddWithValue("@RunwayLength", runwayLength);
                    command.Parameters.AddWithValue("@RunwayMagneticBearing", runwayMagneticBearing);
                    command.Parameters.AddWithValue("@RunwayLatitude", runwayLatitude);
                    command.Parameters.AddWithValue("@RunwayLongitude", runwayLongitude);
                    command.Parameters.AddWithValue("@RunwayGradient", runwayGradient);
                    command.Parameters.AddWithValue("@LtpEllipsoidHeight", ltpEllipsoidHeight);
                    command.Parameters.AddWithValue("@LandingThresholdElevation", landingThresholdElevation);
                    command.Parameters.AddWithValue("@DisplacedThresholdDistance", displacedThresholdDistance);
                    command.Parameters.AddWithValue("@ThresholdCrossingHeight", thresholdCrossingHeight);
                    command.Parameters.AddWithValue("@RunwayWidth", runwayWidth);
                    command.Parameters.AddWithValue("@TchValueIndicator", tchValueIndicator);
                    command.Parameters.AddWithValue("@Stopway", stopway);
                    command.Parameters.AddWithValue("@RunwayDescription", runwayDescription);
                    command.Parameters.AddWithValue("@Cycle", cycle);

                    // Komutun çalıştırılması ve veri ekleme işlemi
                    command.ExecuteNonQuery();

                    // SQL komutunu konsola yazdır
                    Console.WriteLine($"SQL Komutu: {query}");
                }

                return true; // Başarılı durumda true döndür
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Hata: {ex.Message}");
                return false; // Hata durumunda false döndür
            }
        }
    }
}
