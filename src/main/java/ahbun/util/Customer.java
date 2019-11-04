package ahbun.util;

import ahbun.model.Purchase;

public class Customer {
    private  String firstName;
    private  String lastName;
    private  String creditCardNumber;
    private  String customerID;

    public static CustomerBuilder builder() { return new CustomerBuilder(); }
    public static CustomerBuilder builder(final Purchase purchase) {
        CustomerBuilder builder = builder();
        builder.setCreditCardNumber(purchase.getCreditCardNumber());
        builder.setCustomerID(purchase.getCustomerId());
        builder.setFirstName(purchase.getFirstName());
        builder.setLastName(purchase.getLastName());
        return builder;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getCreditCardNumber() {
        return creditCardNumber;
    }

    public String getCustomerID() {
        return customerID;
    }

    @Override
    public String toString() {
        return
                "\"firstName\":\"" + firstName + '\"' +
                        ",\"lastName\":\"" + lastName + '\"' +
                        ",\"creditCardNumber\":\"" + creditCardNumber + '\"' +
                        ",\"customerId\":\"" + customerID + '\"';
    }

    private Customer(final CustomerBuilder builder) {
        firstName = builder.getFirstName();
        lastName = builder.getLastName();
        creditCardNumber = builder.getCreditCardNumber();
        customerID = builder.getCustomerID();
    }

    public static final class CustomerBuilder {
        private  String firstName;
        private  String lastName;
        private  String creditCardNumber;
        private  String customerID;

        public Customer build() {
            return new Customer(this);
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public String getCreditCardNumber() {
            return creditCardNumber;
        }

        public String getCustomerID() {
            return customerID;
        }

        public CustomerBuilder setFirstName(String firstName) {
            this.firstName = firstName;
            return this;
        }

        public CustomerBuilder setLastName(String lastName) {
            this.lastName = lastName;
            return this;
        }

        public CustomerBuilder setCreditCardNumber(String creditCardNumber) {
            this.creditCardNumber = creditCardNumber;
            return this;
        }

        public CustomerBuilder setCustomerID(String customerID) {
            this.customerID = customerID;
            return this;
        }

        private CustomerBuilder(){}
    }

    // limit Customer instantiation to CustomerBuilder
    private Customer(){}
}
