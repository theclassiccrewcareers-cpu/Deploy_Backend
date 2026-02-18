"use client";

import { Home, Users, BookOpen, Calendar, Settings, LogOut } from 'lucide-react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import clsx from 'clsx';
import Image from 'next/image';

const navigation = [
    { name: 'Dashboard', href: '/', icon: Home },
    { name: 'Students', href: '/students', icon: Users },
    { name: 'Resources', href: '/resources', icon: BookOpen },
    { name: 'Schedule', href: '/schedule', icon: Calendar },
    { name: 'Settings', href: '/settings', icon: Settings },
];

export default function Sidebar() {
    const pathname = usePathname();

    return (
        <aside className="fixed top-0 left-0 z-40 w-64 h-screen transition-transform -translate-x-full sm:translate-x-0" aria-label="Sidebar">
            <div className="h-full px-3 py-4 overflow-y-auto bg-[#4D44B5] text-white">
                <div className="flex items-center ps-2.5 mb-8 mt-4 gap-3">
                    <div className="bg-white/10 p-2 rounded-xl">
                        <Image src="/static/noble_logo.png" alt="Logo" width={32} height={32} className="rounded-md" />
                    </div>
                    <span className="self-center text-xl font-bold whitespace-nowrap">Class Bridge</span>
                </div>
                <ul className="space-y-2 font-medium">
                    {navigation.map((item) => {
                        const Icon = item.icon;
                        const isActive = pathname === item.href;
                        return (
                            <li key={item.name}>
                                <Link
                                    href={item.href}
                                    className={clsx(
                                        "flex items-center p-3 rounded-lg group transition-colors",
                                        isActive
                                            ? "bg-white text-[#4D44B5]"
                                            : "text-white/70 hover:bg-white/10 hover:text-white"
                                    )}
                                >
                                    <Icon className={clsx("w-5 h-5 transition duration-75", isActive ? "text-[#4D44B5]" : "text-white/70 group-hover:text-white")} />
                                    <span className="ms-3">{item.name}</span>
                                </Link>
                            </li>
                        );
                    })}
                </ul>

                <div className="absolute bottom-8 left-0 w-full px-3">
                    <button className="flex items-center p-3 text-white/70 rounded-lg hover:bg-white/10 hover:text-white group w-full transition-colors">
                        <LogOut className="w-5 h-5 text-white/70 group-hover:text-white transition duration-75" />
                        <span className="ms-3">Sign Out</span>
                    </button>
                </div>
            </div>
        </aside>
    );
}
