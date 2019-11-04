package ahbun.lib;

import ahbun.model.ShareVolume;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import kafka.utils.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/***
 * FPQTypeAdapter,a TypeAdapter, provides custom serialization/deserialization for
 * FixedSizePriorityQueue<ShareVolume>.
 */
public class FPQTypeAdapter extends TypeAdapter<FixedSizePriorityQueue<ShareVolume>> {
    private Gson gson = new Gson();
    private Logger logger = LoggerFactory.getLogger(FPQTypeAdapter.class);
    @Override
    public void write(JsonWriter out, FixedSizePriorityQueue<ShareVolume> fpq) throws IOException {
        logger.debug("write called ");
        if (fpq == null) {
            out.nullValue();
            return;
        }

        List<ShareVolume> shareVolumeList = new ArrayList<>();
        Iterator<ShareVolume> it = fpq.iterator();
        ShareVolume sv;

        while(it.hasNext()) {
            sv = it.next();
            if (sv != null) {
                shareVolumeList.add(sv);
            }
        }

        out.beginObject();
        out.name("maxSize");
        out.value(fpq.maxSize());
        out.name("shareVolumeList");
        out.beginArray();

        for (ShareVolume shareVolume: shareVolumeList) {
            // add array item - ShareVolume
            out.beginObject(); //shareVolumeList.add(gson.fromJson(in.nextString(), ShareVolume.class));
            out.name("industry");
            out.value(shareVolume.getIndustry());
            out.name("symbol");
            out.value(shareVolume.getSymbol());
            out.name("volume");
            out.value(shareVolume.getVolume());
            out.endObject();
        }
        out.endArray();
        out.endObject();
    }


    @Override
    public FixedSizePriorityQueue<ShareVolume> read(JsonReader in) throws IOException {
        if (in.peek() == JsonToken.NULL) {
            in.nextNull();
            return null;
        }

        logger.debug("read called ");
        int maxSize = 0;
        String fieldName="";
        List<ShareVolume> shareVolumeList = null;
        JsonToken token;

        while(in.hasNext()) {
            token = in.peek();
            if (token == JsonToken.BEGIN_OBJECT) {
                in.beginObject();
            } else if (token.equals(JsonToken.NAME)) {
               fieldName = in.nextName();
           }

           if ("maxSize".equals(fieldName)) {
               maxSize = in.nextInt();
           } else if ("shareVolumeList".equals(fieldName)) {
               shareVolumeList = readShareVolumeList(in);
           }
        }
        in.endObject();
        // rebuild the FixedSizePriorityQueue
        Comparator<ShareVolume> comparator = (sv1, sv2) -> sv2.getVolume() - sv1.getVolume();

        FixedSizePriorityQueue<ShareVolume> fpq =
                new FixedSizePriorityQueue<>(comparator, maxSize);

        if (shareVolumeList != null) {
            for (ShareVolume sv : shareVolumeList) {
                fpq.add(sv);
            }
        }
        return fpq;
    }

    private List<ShareVolume> readShareVolumeList(JsonReader in) throws IOException {
        List<ShareVolume> shareVolumeList = new ArrayList<>();

        in.beginArray();
        while (in.hasNext()) {
            shareVolumeList.add(readObject(in));
        }
        in.endArray();
        return shareVolumeList;
    }

    private ShareVolume readObject(JsonReader in) throws IOException {
        String fieldName = "";
        String industry = "";
        String symbol = "";
        int volume = 0;

        in.beginObject();
        while (in.hasNext()) {
                JsonToken token = in.peek();
                if (token.equals(JsonToken.NAME)) {
                    fieldName = in.nextName();
                }

                if (fieldName.equals("industry")) {
                    industry = in.nextString();
                } else if (fieldName.equals("symbol")) {
                    symbol = in.nextString();
                } else if (fieldName.equals("volume")) {
                    volume = in.nextInt();
                }
        }
        in.endObject();

        return new ShareVolume(industry, symbol, volume);
    }
}
