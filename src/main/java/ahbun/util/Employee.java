package ahbun.util;

public class Employee {
    private String employeeId;
    private String zipCode;

    public static EmployeeBuilder builder() { return new EmployeeBuilder(); }

    public String getEmployeeId() {
        return employeeId;
    }

    public String getZipCode() {
        return zipCode;
    }

    //"employeeId":"emp-01001","zipCode"
    @Override
    public String toString() {
        return String.format("\"employeeId\":\"" + getEmployeeId() +
                "\",\"zipCode\":\"" + getZipCode() + "\"");
    }

    private Employee(EmployeeBuilder builder) {
        this.employeeId = builder.getEmployeeId();
        this.zipCode = builder.getZipCode();
    }

    private Employee(){}

    public static final class EmployeeBuilder {
        private String employeeId;
        private String zipCode;

        public Employee build() {
            return new Employee(this);
        }


        private EmployeeBuilder(){}

        public String getEmployeeId() {
            return employeeId;
        }

        public EmployeeBuilder setEmployeeId(String employeeId) {
            this.employeeId = employeeId;
            return this;
        }

        public String getZipCode() {
            return zipCode;
        }

        public EmployeeBuilder setZipCode(String zipCode) {
            this.zipCode = zipCode;
            return this;
        }
    }


}
