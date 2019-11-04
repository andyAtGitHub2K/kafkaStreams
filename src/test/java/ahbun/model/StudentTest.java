package ahbun.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StudentTest {
    Student student;

    @Before
    public void setUp() throws Exception {
        student = new Student();
        student.setRollNo(10);
        List<ShareVolume> subjects = new ArrayList<>();
        ShareVolume sv = new ShareVolume("a", "bc", 100);
        ShareVolume svB = new ShareVolume("b", "xx", 500);
        subjects.add(sv);
        subjects.add(svB);
        student.setShareVolumeList(subjects);
    }

    @Test
    public void testStudentWithListDeserialize() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(Student.class, new StudentAdapter());
        builder.setPrettyPrinting();

        Gson gson = builder.create();

        String studentJson = gson.toJson(student);
        System.out.println(studentJson);

        String json = "{\"rollNo\":10,\"shareVolumeList\":[{\"industry\":\"a\",\"symbol\":\"xx\",\"volume\":100},{\"industry\":\"B\",\"symbol\":\"DD\",\"volume\":500}]}";
        Student toStudent = gson.fromJson(json, Student.class);
        System.out.println(toStudent);
    }
}