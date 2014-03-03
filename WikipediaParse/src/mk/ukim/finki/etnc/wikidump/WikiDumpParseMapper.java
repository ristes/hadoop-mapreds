package mk.ukim.finki.etnc.wikidump;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

public class WikiDumpParseMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String xmlString = value.toString();

		SAXBuilder builder = new SAXBuilder();
		Reader in = new StringReader(xmlString);
		try {

			Document doc = builder.build(in);
			Element root = doc.getRootElement();

			List<Element> revisions = root.getChildren("revision");
			if (!revisions.isEmpty()) {
				root = revisions.get(revisions.size() - 1);

				String id = root.getChild("id").getTextTrim();

				Element textEl = root.getChild("text");

				if (textEl != null) {
					String text = textEl.getTextTrim();
					output.collect(new Text(id), new Text(text));
				} else {
					System.out.println("ID: " + id);
				}
			}

		} catch (JDOMException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (in != null) {
				in.close();
			}
		}

	}

}