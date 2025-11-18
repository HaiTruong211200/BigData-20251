import type { Metadata } from "next";
import { Roboto } from "next/font/google";

// @ts-ignore: CSS module type declarations are not present in this project
import "./styles/globals.css";

const roboto = Roboto({
    display: 'swap',
    subsets: ["latin"],
    weight: ["300", "400", "500", "700"],
});

export const metadata: Metadata = {
    title: "Airline Flight Analysis",
    description: "Dashboard for Airline Flight Delays Analysis",
};

export default function RootLayout({
    children,
}: Readonly<{
    children: React.ReactNode;
}>) {
    return (
        <html lang="en" className={roboto.className}>
            <body>
                {children}
            </body>
        </html>
    );
}
