package ahbun.model;

import java.util.List;

public class Student {
    private int rollNo;
    private List<ShareVolume> shareVolumeList;

    public int getRollNo() {
        return rollNo;
    }

    public Student setRollNo(int rollNo) {
        this.rollNo = rollNo;
        return this;
    }


    public List<ShareVolume> getShareVolumeList() {
        return shareVolumeList;
    }

    public Student setShareVolumeList(List<ShareVolume> shareVolumeList) {
        this.shareVolumeList = shareVolumeList;
        return this;
    }

    @Override
    public String toString() {
        return "Student{" +
                "rollNo=" + rollNo +
                ", subjects=" + shareVolumeList +
                '}';
    }
}
