package ahbun.model;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StudentAdapter extends TypeAdapter<Student> {
    @Override
    public void write(JsonWriter out, Student student) throws IOException {
        out.beginObject();
        out.name("rollNo");
        out.value(student.getRollNo());
        out.name("shareVolumeList");
        out.beginArray();
        for (ShareVolume shareVolume: student.getShareVolumeList()) {
            out.beginObject();
            out.name("industry");
            out.value(shareVolume.getIndustry());
            out.name("synbol");
            out.value(shareVolume.getSymbol());
            out.name("volume");
            out.value(shareVolume.getVolume());
            out.endObject();
        }
        out.endArray();
        out.endObject();
    }

    @Override
    public Student read(JsonReader in) throws IOException {
        Student student = new Student();
        String fieldName = null;
        in.beginObject();
        while (in.hasNext()) {
            JsonToken token = in.peek();

            if (token.equals(JsonToken.NAME)) {
                fieldName = in.nextName();
            }

            if ("rollNo".equals(fieldName)) {
                student.setRollNo(in.nextInt());
            } else if ("shareVolumeList".equals(fieldName)) {
                List<ShareVolume> shareVolumeList = readList(in);
                student.setShareVolumeList(shareVolumeList);
            }
        }

        in.endObject();
        return student;
    }

    private List<ShareVolume> readList(JsonReader in) throws IOException {
        List<ShareVolume> list = new ArrayList<>();
        in.beginArray();
        while (in.hasNext()) {
            ShareVolume shareVolume  = readObject(in);
            list.add(shareVolume);
        }
        in.endArray();
        return list;
    }

    private ShareVolume readObject(JsonReader in)  throws IOException {
        String industry="";
        String symbol="";
        int volume=0;
        String fieldName="";

        in.beginObject();
        while (in.hasNext()) {
            JsonToken token = in.peek();
            if (token.equals(JsonToken.NAME)) {
                fieldName = in.nextName();
            }

            if ("industry".equals(fieldName)) {
                industry = in.nextString();
            } else if ("symbol".equals(fieldName)) {
                symbol = in.nextString();
            } else if ("volume".equals(fieldName)) {
                volume = in.nextInt();
            }
        }
        in.endObject();

        return new ShareVolume(industry, symbol, volume);
    }
}
