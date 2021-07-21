package eu.profinit.manta.graphplayground.model.manta;

public class LicenseException extends RuntimeException {
    private static final long serialVersionUID = 7049929547183195295L;

    public LicenseException() {
    }

    public LicenseException(String message) {
        super(message);
    }

    public LicenseException(String message, Throwable cause) {
        super(message, cause);
    }

    public LicenseException(Throwable cause) {
        super(cause);
    }
}