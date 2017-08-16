import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikipediaPageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Pattern Patterns = Pattern.compile("\\[.+?\\]");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = parseLink(value);
        String pageString = val[0];

        if(isValidPage(pageString)) return;

        Text pg = new Text(pageString.replace(' ', '_'));
        Matcher matcher = Patterns.matcher(val[1]);
        
        while (matcher.find()) {
            String otherPage = matcher.group();
            otherPage = getWikiPage(otherPage);
            if(otherPage == null || otherPage.isEmpty()) 
                continue;
            context.write(pg, new Text(otherPage));
        }
    }
    
    private boolean isValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String getWikiPage(String Link){
        if(checkWikiLink(Link)) return null;
        
        int start = Link.startsWith("[[") ? 2 : 1;
        int end = Link.indexOf("]");
        int pos = Link.indexOf("|");

        if(pos > 0){
            end = pos;
        }
        
        int pt = Link.indexOf("#");
        if(pt > 0){
            end = pt;
        }
        
        Link = Link.substring(start, end);
        Link = Link.replaceAll("\\s", "_");
        Link = Link.replaceAll(",", "");
        Link = remAmp(Link);
        
        return Link;
    }
    
    private String remAmp(String LinkText) {
        if(LinkText.contains("&amp;"))
            return LinkText.replace("&amp;", "&");

        return LinkText;
    }

    private String[] parseLink(Text value) throws CharacterCodingException {
        String[] val = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; 
        
        val[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        val[1] = Text.decode(value.getBytes(), start, end-start);
        
        return val;
    }

    private boolean checkWikiLink(String Link) {
        int start = 1;
        if(Link.startsWith("[[")){
            start = 2;
        }
        
        if( Link.length() < start+2 || Link.length() > 100) return true;
        char firstChar = Link.charAt(start);
        
        if( firstChar == '{') return true;
        if( firstChar == '&') return true;
        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '-') return true;
        if( firstChar == '\'') return true;
        
        if( Link.contains(":")) return true;
        if( Link.contains(",")) return true;
        if( Link.contains("&")) return true;
	        
        return false;
	}
}
