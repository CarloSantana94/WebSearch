import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class TSVToCSVConverter {

	private static String headerArea = " \"id\",\"gid\",\"name\",\"type\",\"edits_pending\",\"last_updated\",\"id\",\"begin_date_year\",\"begin_date_month\",\"begin_date_day\",\"end_date_year\",\"end_date_month\",\"end_date_day\",\"ended\",\"comment\"\n";
	private static String headerAreaType = " \"id\",\"name\",\"parent\",\"child_order\",\"description\",\"gid\"\n";
	private static String headerArtist = " \"id\",\"gid\",\"name\",\"sort_name\",\"begin_date_year\",\"begin_date_month\",\"begin_date_day\",\"end_date_year\",\"end_date_month\",\"end_date_day\",\"type\",\"area\",\"gender\",\"comment\",\"edits_pending\",\"last_updated\",\"ended\",\"begin_area\",\"end_area\" \n";
	private static String headerArtistCredit = " \"id\",\"name\",\"artist_count\",\"ref_count\",\"created\",\"edits_pending\" \n";
	private static String headerArtistCreditName = " \"artist_credit\",\"position\",\"artist\",\"name\",\"join_phrase\"\n";
	private static String headerGender = " \"id\",\"name\",\"parent\",\"child_order\",\"child_order\",\"child_order\"\n";
	private static String headerLabel = " \"id\", \"gid\", \"name\", \"begin_date_year\", \"begin_date_month\", \"begin_date_day\", \"end_date_year\", \"end_date_month\", \"end_date_day\", \"label_code\", \"type\", \"area\", \"comment\", \"edits_pending\", \"last_updated\", \"ended\"";
	private static String headerMedium = " \"id\", \"release\", \"position\", \"format\", \"name\", \"edits_pending\", \"last_updated\", \"track_count\"\n";
	private static String headerMediumFormat = "\"id\",\"name\",\"parent\",\"child_order\",\"year\",\"has_discids\",\"description\",\"gid\"\n";
	private static String headerRecording = " \"id\", \"gid\", \"name\", \"artist_credit\", \"length\", \"comment\", \"edits_pending\", \"last_updated\", \"video\" \n";
	private static String headerRelease = " \"id\", \"gid\", \"name\", \"artist_credit\", \"release_group\", \"status\", \"packaging\", \"language\", \"script\", \"barcode\", \"comment\", \"edits_pending\", \"quality\", \"last_updated\" \n";
	private static String headerTrack =" \"id\", \"gid\", \"recording\", \"medium\", \"position\", \"number\", \"name\", \"artist_credit\", \"length\", \"edits_pending\", \"last_updated\", \"is_data_track\" \n";

	public static void main(String[] args) throws Exception {

//		String tsvFilePathArea = "D:\Databases\mbdump\Area"
//		String csvFilePathArea = "D:\Databases\mbdump\Area.csv"
		String tsvFilePathArea = "AreaPath";
		String csvFilePathArea = "AreaPath.csv";
		
		String tsvFilePathAreaType = "AreaTypePath";
		String csvFilePathAreaType = "AreaPathType.csv";
		
		String tsvFilePathArtist = "ArtistPath";
		String csvFilePathArtist = "ArtistPath.csv";
		
		String tsvFilePathArtistCredit = "ArtistCreditPath";
		String csvFilePathArtistCredit = "ArtistCredit.csv";
		
		String tsvFilePathArtistCreditName = "ArtistCreditNamePath";
		String csvFilePathArtistCreditName = "ArtistCreditNamePath.csv";
		
		String tsvFilePathGender = "GenderPath";
		String csvFilePathGender = "GenderPath.csv";
		
		String tsvFilePathLabel = "LabelPath";
		String csvFilePathLabel = "LabelPath.csv";
		
		String tsvFilePathMedium = "MediumPath";
		String csvFilePathMedium = "MediumPath.csv";
		
		String tsvFilePathMediumFormat = "MediumFormatPath";
		String csvFilePathMediumFormat = "MediumFormatPath.csv";
		
		String tsvFilePathRecording = "RecordingPath";
		String csvFilePathRecording = "RecordingPath.csv";
		
		String tsvFilePathRelease = "ReleasePath";
		String csvFilePathRelease = "ReleasePath.csv";
		
		String tsvFilePathTrack = "TrackPath";
		String csvFilePathTrack = "TrackPath.csv";

		convertTSVToCSVFile(csvFilePathArea, tsvFilePathArea, headerArea);
		convertTSVToCSVFile(csvFilePathAreaType, tsvFilePathAreaType, headerAreaType);
		convertTSVToCSVFile(csvFilePathArtist, tsvFilePathArtist, headerArtist);
		convertTSVToCSVFile(csvFilePathArtistCredit, tsvFilePathArtistCredit, headerArtistCredit);
		convertTSVToCSVFile(csvFilePathArtistCreditName, tsvFilePathArtistCreditName, headerArtistCreditName);
		convertTSVToCSVFile(csvFilePathGender, tsvFilePathGender, headerGender);
		convertTSVToCSVFile(csvFilePathLabel, tsvFilePathLabel, headerLabel);
		convertTSVToCSVFile(csvFilePathMedium, tsvFilePathMedium, headerMedium);
		convertTSVToCSVFile(csvFilePathMediumFormat, tsvFilePathMediumFormat, headerMediumFormat);
		convertTSVToCSVFile(csvFilePathRecording, tsvFilePathRecording, headerRecording);
		convertTSVToCSVFile(csvFilePathRelease, tsvFilePathRelease, headerRelease);
		convertTSVToCSVFile(csvFilePathTrack, tsvFilePathTrack, headerTrack);

	}

	private static void convertTSVToCSVFile(String csvFilePath, String tsvFilePath, String header) throws IOException {

		String[] tokenizer;
		try (BufferedReader br = new BufferedReader(new FileReader(tsvFilePath));
				PrintWriter writer = new PrintWriter(new FileWriter(csvFilePath));) {

			writer.write(header);
			
			int i = 0;
			for (String line; (line = br.readLine()) != null;) {

				i++;
				if (i % 10000 == 0) {
					System.out.println("Processed: " + i);

				}
				tokenizer = line.split("\t");

				String csvLine = "";
				String token;
				for (int j = 0; j < tokenizer.length; j++) {
					token = tokenizer[j].replaceAll("\"", "'");
					csvLine += "\"" + token + "\",";
				}

				if (csvLine.endsWith(",")) {
					csvLine = csvLine.substring(0, csvLine.length() - 1);
				}

				writer.write(csvLine + System.getProperty("line.separator"));

			}

		}
	}

}