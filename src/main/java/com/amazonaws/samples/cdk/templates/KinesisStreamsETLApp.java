package com.amazonaws.samples.cdk.templates;



import software.amazon.awscdk.core.App;

public class KinesisStreamsETLApp {
	
	public static void main(final String[] args) {
        App app = new App();

        new KinesisStreamETLStack(app, "KinesisStreamETLStack");

        app.synth();
    }

}
