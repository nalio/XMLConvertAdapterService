package com.progress.codeshare.esbservice.xmlConvertAdapter;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;

import com.sonicsw.xq.XQConstants;
import com.sonicsw.xq.XQEnvelope;
import com.sonicsw.xq.XQInitContext;
import com.sonicsw.xq.XQMessage;
import com.sonicsw.xq.XQMessageFactory;
import com.sonicsw.xq.XQParameters;
import com.sonicsw.xq.XQPart;
import com.sonicsw.xq.XQServiceContext;
import com.sonicsw.xq.XQServiceEx;
import com.sonicsw.xq.XQServiceException;
import com.unidex.xflat.XmlConvert;

public final class XMLConvertAdapterService implements XQServiceEx {
	private static final String MODE_FLAT_TO_FLAT = "Flat to Flat";
	private static final String MODE_FLAT_TO_XML = "Flat to XML";
	private static final String MODE_XML_TO_FLAT = "XML to Flat";

	private static final String PARAM_KEEP_ORIGINAL_PART = "keepOriginalPart";
	private static final String PARAM_MESSAGE_PART = "messagePart";
	private static final String PARAM_MODE = "mode";
	private static final String PARAM_SRC_SCHEMA = "srcSchema";
	private static final String PARAM_TARGET_SCHEMA = "targetSchema";

	public void destroy() {
	}

	public void init(XQInitContext ctx) {
	}

	public void service(final XQServiceContext ctx) throws XQServiceException {

		try {
			final XQParameters params = ctx.getParameters();

			final String mode = params.getParameter(PARAM_MODE,
					XQConstants.PARAM_STRING);

			final XQMessageFactory msgFactory = ctx.getMessageFactory();

			final int messagePart = params.getIntParameter(PARAM_MESSAGE_PART,
					XQConstants.PARAM_STRING);
			final boolean keepOriginalPart = params.getBooleanParameter(
					PARAM_KEEP_ORIGINAL_PART, XQConstants.PARAM_STRING);
			final String srcSchema = params.getParameter(PARAM_SRC_SCHEMA,
					XQConstants.PARAM_STRING);

			final XmlConvert converter = new XmlConvert(new StringReader(
					srcSchema), false);

			if (MODE_FLAT_TO_FLAT.equals(mode)) {
				final String targetSchema = params.getParameter(
						PARAM_TARGET_SCHEMA, XQConstants.PARAM_STRING);

				while (ctx.hasNextIncoming()) {
					final XQEnvelope env = ctx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final XQMessage newMsg = msgFactory.createMessage();

					final Iterator headerNameIterator = origMsg
							.getHeaderNames();

					while (headerNameIterator.hasNext()) {
						final String headerName = (String) headerNameIterator
								.next();

						newMsg.setHeaderValue(headerName, origMsg
								.getHeaderValue(headerName));
					}

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQPart origPart = origMsg.getPart(i);

							/* Decide whether to keep the original part or not */
							if (keepOriginalPart) {
								origPart.setContentId("original_part_" + i);

								newMsg.addPart(origPart);
							}

							final XQPart newPart = newMsg.createPart();

							final Writer writer = new StringWriter();

							final String content = (String) origPart
									.getContent();

							converter.flatToFlat(new StringReader(content),
									new StringReader(targetSchema), writer);

							newPart.setContent(writer.toString(),
									XQConstants.CONTENT_TYPE_TEXT);

							newMsg.addPart(newPart);
						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

					env.setMessage(newMsg);

					final Iterator addressIterator = env.getAddresses();

					if (addressIterator.hasNext())
						ctx.addOutgoing(env);

				}

			} else if (MODE_FLAT_TO_XML.equals(mode)) {

				while (ctx.hasNextIncoming()) {
					final XQEnvelope env = ctx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final XQMessage newMsg = msgFactory.createMessage();

					final Iterator nameIterator = origMsg.getHeaderNames();

					while (nameIterator.hasNext()) {
						final String name = (String) nameIterator.next();

						newMsg.setHeaderValue(name, origMsg
								.getHeaderValue(name));
					}

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQPart origPart = origMsg.getPart(i);

							/* Decide whether to keep the original part or not */
							if (keepOriginalPart) {
								origPart.setContentId("original_part_" + i);

								newMsg.addPart(origPart);
							}

							final XQPart newPart = newMsg.createPart();

							final Writer writer = new StringWriter();

							final String content = (String) origPart
									.getContent();

							converter.flatToXml(new StringReader(content),
									writer);

							newPart.setContent(writer.toString(),
									XQConstants.CONTENT_TYPE_XML);

							newMsg.addPart(newPart);
						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

					env.setMessage(newMsg);

					final Iterator addressIterator = env.getAddresses();

					if (addressIterator.hasNext())
						ctx.addOutgoing(env);

				}

			} else if (MODE_XML_TO_FLAT.equals(mode)) {

				while (ctx.hasNextIncoming()) {
					final XQEnvelope env = ctx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final XQMessage newMsg = msgFactory.createMessage();

					final Iterator nameIterator = origMsg.getHeaderNames();

					while (nameIterator.hasNext()) {
						final String name = (String) nameIterator.next();

						newMsg.setHeaderValue(name, origMsg
								.getHeaderValue(name));
					}

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQPart origPart = origMsg.getPart(i);

							/* Decide whether to keep the original part or not */
							if (keepOriginalPart) {
								origPart.setContentId("original_part_" + i);

								newMsg.addPart(origPart);
							}

							final XQPart newPart = newMsg.createPart();

							final Writer writer = new StringWriter();

							final String content = (String) origPart
									.getContent();

							converter.xmlToFlat(new StringReader(content),
									writer);

							newPart.setContent(writer.toString(),
									XQConstants.CONTENT_TYPE_TEXT);

							newMsg.addPart(newPart);
						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

					env.setMessage(newMsg);

					final Iterator addressIterator = env.getAddresses();

					if (addressIterator.hasNext())
						ctx.addOutgoing(env);

				}

			}

		} catch (final Exception e) {
			throw new XQServiceException(e);
		}

	}

	public void start() {
	}

	public void stop() {
	}

}