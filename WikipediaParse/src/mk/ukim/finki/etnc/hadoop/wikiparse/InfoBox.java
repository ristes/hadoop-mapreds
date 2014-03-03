package mk.ukim.finki.etnc.hadoop.wikiparse;

/**
 * A class abstracting Wiki infobox
 * 
 * @author Delip Rao
 */
public class InfoBox {
	String infoBoxWikiText = null;

	InfoBox(String infoBoxWikiText) {
		this.infoBoxWikiText = infoBoxWikiText;
	}

	public String dumpRaw() {
		return infoBoxWikiText;
	}
}
