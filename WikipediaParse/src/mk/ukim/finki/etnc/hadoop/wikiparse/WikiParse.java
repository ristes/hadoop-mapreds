package mk.ukim.finki.etnc.hadoop.wikiparse;

public class WikiParse {

	public static String getPlainText(String wikiText) {
		String text = wikiText.replaceAll("&gt;", ">");
		text = text.replaceAll("&lt;", "<");
		text = text.replaceAll("<ref>.*?</ref>", " ");
		text = text.replaceAll("</?.*?>", " ");
		text = text.replaceAll("\\{\\{.*?\\}\\}", " ");
		text = text.replaceAll("\\[\\[.*?:.*?\\]\\]", " ");
		text = text.replaceAll("\\[\\[(.*?)\\]\\]", "$1");
		text = text.replaceAll("\\s(.*?)\\|(\\w+\\s)", " $2");
		text = text.replaceAll("\\[.*?\\]", " ");
		text = text.replaceAll("\\'+", "");
		return text;
	}

	public static InfoBox parseInfoBox(String wikiText) {
		String INFOBOX_CONST_STR = "{{Infobox";
		int startPos = wikiText.indexOf(INFOBOX_CONST_STR);
		if (startPos < 0)
			return null;
		int bracketCount = 2;
		int endPos = startPos + INFOBOX_CONST_STR.length();
		for (; endPos < wikiText.length(); endPos++) {
			switch (wikiText.charAt(endPos)) {
			case '}':
				bracketCount--;
				break;
			case '{':
				bracketCount++;
				break;
			default:
			}
			if (bracketCount == 0)
				break;
		}
		if (endPos + 1 >= wikiText.length())
			return null;
		// This happens due to malformed Infoboxes in wiki text. See Issue #10
		// Giving up parsing is the easier thing to do.
		String infoBoxText = wikiText.substring(startPos, endPos + 1);
		infoBoxText = stripCite(infoBoxText); // strip clumsy {{cite}} tags
		// strip any html formatting
		infoBoxText = infoBoxText.replaceAll("&gt;", ">");
		infoBoxText = infoBoxText.replaceAll("&lt;", "<");
		infoBoxText = infoBoxText.replaceAll("<ref.*?>.*?</ref>", " ");
		infoBoxText = infoBoxText.replaceAll("</?.*?>", " ");
		return new InfoBox(infoBoxText);
	}

	private static String stripCite(String text) {
		
		String CITE_CONST_STR = "{{cite";
		int startPos = text.indexOf(CITE_CONST_STR);
		if (startPos < 0)
			return text;
		int bracketCount = 2;
		int endPos = startPos + CITE_CONST_STR.length();
		for (; endPos < text.length(); endPos++) {
			switch (text.charAt(endPos)) {
			case '}':
				bracketCount--;
				break;
			case '{':
				bracketCount++;
				break;
			}
			if (bracketCount == 0)
				break;
		}
		text = text.substring(0, startPos - 1) + text.substring(endPos);
		return stripCite(text);
	}

}
